use super::Storage;
use crate::{Command, Entry, Metadata};
use async_trait::async_trait;
use bincode::Options;
use byteorder::{WriteBytesExt, LE};
use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{io::SeekFrom, marker::PhantomData, path::PathBuf};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tokio_util::codec::{Decoder, Encoder, Framed, FramedRead};

#[derive(Debug, thiserror::Error)]
pub enum DiskStorageError {
    #[error("Index is too large")]
    IndexTooLarge,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Bincode(#[from] Box<bincode::ErrorKind>),
}

pub struct DiskStorage<C> {
    dir_path: PathBuf,
    dir: Option<File>,
    framed: Framed<File, EntryCodec<C>>,
    offsets: Vec<u64>,
    current_offset: u64,
}

#[async_trait]
impl<C> Storage for DiskStorage<C>
where
    for<'de> C: Command + Serialize + Deserialize<'de>,
{
    type Command = C;
    type Error = DiskStorageError;

    async fn load(&mut self) -> Result<Metadata, Self::Error> {
        match File::open(self.dir_path.join("metadata")).await {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).await?;
                Ok(bincode::DefaultOptions::new().deserialize(&bytes)?)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Metadata::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn num_entries(&self) -> usize {
        self.offsets.len()
    }

    async fn entry(&mut self, index: u64) -> Result<Option<Entry<Self::Command>>, Self::Error> {
        let index: usize = if index > 0 {
            (index - 1)
                .try_into()
                .map_err(|_| DiskStorageError::IndexTooLarge)?
        } else {
            return Ok(None);
        };
        let Some(&offset) = self.offsets.get(index) else {
            return Ok(None);
        };
        self.flush().await?;
        self.file().seek(SeekFrom::Start(offset)).await?;
        self.framed.read_buffer_mut().clear();
        self.framed.next().await.transpose().map_err(Into::into)
    }

    async fn entries(&mut self, start: u64) -> Result<Vec<Entry<Self::Command>>, Self::Error> {
        assert!(start > 0);
        let index: usize = (start - 1)
            .try_into()
            .map_err(|_| DiskStorageError::IndexTooLarge)?;
        let Some(&offset) = self.offsets.get(index) else {
            return Ok(Vec::new());
        };
        let num_entries_to_read = self.offsets.len() - index;
        self.flush().await?;
        self.file().seek(SeekFrom::Start(offset)).await?;
        self.framed.read_buffer_mut().clear();
        let mut entries = Vec::with_capacity(num_entries_to_read);
        while let Some(entry) = self.framed.next().await {
            entries.push(entry?);
        }
        Ok(entries)
    }

    async fn append_entries(
        &mut self,
        entries: &[Entry<Self::Command>],
    ) -> Result<(), Self::Error> {
        self.file().seek(SeekFrom::End(0)).await?;
        for entry in entries {
            let size = bincode::DefaultOptions::new().serialized_size(entry)?;
            let offset = self.current_offset;
            self.framed.feed(EncoderItem { inner: entry, size }).await?;
            self.current_offset += size + HEADER_SIZE as u64;
            self.offsets.push(offset);
        }
        self.persist_entries().await?;
        Ok(())
    }

    async fn truncate_entries(&mut self, index: u64) -> Result<(), Self::Error> {
        assert!(index > 0);
        let index: usize = index
            .try_into()
            .map_err(|_| DiskStorageError::IndexTooLarge)?;
        let Some(&offset) = self.offsets.get(index) else {
            return Ok(());
        };
        self.flush().await?;
        self.file().set_len(offset).await?;
        self.offsets.truncate(index);
        self.persist_entries().await?;
        Ok(())
    }

    async fn persist_metadata(&mut self, metadata: &Metadata) -> Result<(), Self::Error> {
        let bytes = bincode::DefaultOptions::new().serialize(metadata)?;
        let tmp_filename = self.dir_path.join("metadata.tmp");
        {
            let mut tmp_file = File::create(&tmp_filename).await?;
            tmp_file.write_all(&bytes).await?;
            tmp_file.sync_data().await?;
        }
        tokio::fs::rename(tmp_filename, self.dir_path.join("metadata")).await?;
        if let Some(dir) = &self.dir {
            dir.sync_all().await?;
        }
        Ok(())
    }

    async fn persist_entries(&mut self) -> Result<(), Self::Error> {
        self.flush().await?;
        self.file().sync_data().await?;
        Ok(())
    }
}

impl<C> DiskStorage<C> {
    pub async fn new(dir_path: impl Into<PathBuf>) -> Result<Self, DiskStorageError> {
        let dir_path = dir_path.into();
        tokio::fs::create_dir_all(&dir_path).await?;
        let dir = File::open(&dir_path).await.ok();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(dir_path.join("log"))
            .await?;
        let mut size_reader = FramedRead::new(&mut file, SizeDecoder::default());
        let mut offsets = Vec::new();
        let mut current_offset = 0;
        while let Some(size) = size_reader.next().await {
            offsets.push(current_offset);
            current_offset += size? + HEADER_SIZE as u64;
        }
        let framed = Framed::new(file, EntryCodec::default());
        Ok(Self {
            dir_path,
            dir,
            framed,
            offsets,
            current_offset,
        })
    }

    fn file(&mut self) -> &mut File {
        self.framed.get_mut()
    }
}

impl<C: Serialize> DiskStorage<C> {
    async fn flush(&mut self) -> Result<(), DiskStorageError> {
        if !self.framed.write_buffer().is_empty() {
            self.file().seek(SeekFrom::End(0)).await?;
            self.framed.flush().await?;
        }
        Ok(())
    }
}

struct EntryCodec<C> {
    decoder: InnerDecoder,
    phantom: PhantomData<C>,
}

impl<C> Default for EntryCodec<C> {
    fn default() -> Self {
        Self {
            decoder: Default::default(),
            phantom: Default::default(),
        }
    }
}

const HEADER_SIZE: usize = std::mem::size_of::<u64>();

struct EncoderItem<'a, C> {
    inner: &'a Entry<C>,
    size: u64,
}

impl<C> Encoder<EncoderItem<'_, C>> for EntryCodec<C>
where
    C: Serialize,
{
    type Error = Box<bincode::ErrorKind>;

    fn encode(&mut self, item: EncoderItem<'_, C>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut writer = dst.writer();
        writer.write_u64::<LE>(item.size)?;
        bincode::DefaultOptions::new().serialize_into(writer, item.inner)?;
        Ok(())
    }
}

impl<C> Decoder for EntryCodec<C>
where
    for<'de> C: Deserialize<'de>,
{
    type Item = Entry<C>;
    type Error = Box<bincode::ErrorKind>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decoder
            .decode(src, |s| bincode::DefaultOptions::new().deserialize_from(s))
    }
}

#[derive(Default)]
struct SizeDecoder {
    inner: InnerDecoder,
}

impl Decoder for SizeDecoder {
    type Item = u64;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src, |s| Ok(s.len() as u64))
    }
}

#[derive(Default)]
struct InnerDecoder {
    size: Option<usize>,
}

impl InnerDecoder {
    fn decode<T, E, F>(&mut self, src: &mut BytesMut, f: F) -> Result<Option<T>, E>
    where
        F: Fn(&[u8]) -> Result<T, E>,
    {
        let size = match self.size {
            Some(size) => size,
            None => {
                if src.len() < HEADER_SIZE {
                    return Ok(None);
                }
                src.get_u64_le() as usize
            }
        };
        if src.len() < size {
            self.size = Some(size);
            src.reserve(size);
            return Ok(None);
        }
        let decoded = f(&src[..size])?;
        src.advance(size);
        self.size = None;
        Ok(Some(decoded))
    }
}
