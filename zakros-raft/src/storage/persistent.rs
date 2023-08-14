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
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

#[derive(Debug, thiserror::Error)]
pub enum PersistentStorageError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Bincode(#[from] Box<bincode::ErrorKind>),
}

pub struct PersistentStorage<C> {
    dir_path: PathBuf,
    dir: Option<File>,
    writer: FramedWrite<File, EntryEncoder<Entry<C>>>,
    offsets: Vec<u64>,
    current_offset: u64,
}

#[async_trait]
impl<C> Storage for PersistentStorage<C>
where
    for<'de> C: Command + Serialize + Deserialize<'de>,
{
    type Command = C;
    type Error = PersistentStorageError;

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
            (index - 1).try_into().unwrap()
        } else {
            return Ok(None);
        };
        let Some(&offset) = self.offsets.get(index) else {
            return Ok(None);
        };
        self.flush_writer().await?;
        self.file().seek(SeekFrom::Start(offset)).await?;
        let mut reader = FramedRead::new(self.file(), EntryDecoder::default());
        reader.next().await.transpose().map_err(Into::into)
    }

    async fn entries(&mut self, start: u64) -> Result<Vec<Entry<Self::Command>>, Self::Error> {
        assert!(start > 0);
        let index: usize = (start - 1).try_into().unwrap();
        let Some(&offset) = self.offsets.get(index) else {
            return Ok(Vec::new());
        };
        self.flush_writer().await?;
        self.file().seek(SeekFrom::Start(offset)).await?;
        let mut reader = FramedRead::new(self.file(), EntryDecoder::default());
        let mut entries = Vec::new();
        while let Some(entry) = reader.next().await {
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
            self.writer.feed(EncoderItem { inner: entry, size }).await?;
            self.current_offset += size + HEADER_SIZE as u64;
            self.offsets.push(offset);
        }
        self.persist_entries().await?;
        Ok(())
    }

    async fn truncate_entries(&mut self, index: u64) -> Result<(), Self::Error> {
        assert!(index > 0);
        let index: usize = index.try_into().unwrap();
        let Some(&offset) = self.offsets.get(index) else {
            return Ok(());
        };
        self.flush_writer().await?;
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
        self.flush_writer().await?;
        self.file().sync_data().await?;
        Ok(())
    }
}

impl<C> PersistentStorage<C> {
    pub async fn new(dir_path: impl Into<PathBuf>) -> Result<Self, PersistentStorageError> {
        let dir_path = dir_path.into();
        tokio::fs::create_dir_all(&dir_path).await?;
        let dir = File::open(&dir_path).await.ok();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(dir_path.join("log"))
            .await?;
        let mut reader = FramedRead::new(&mut file, SizeDecoder::default());
        let mut offsets = Vec::new();
        let mut current_offset = 0;
        while let Some(size) = reader.next().await {
            offsets.push(current_offset);
            current_offset += size? + HEADER_SIZE as u64;
        }
        let writer = FramedWrite::new(file, EntryEncoder::default());
        Ok(Self {
            dir_path,
            dir,
            writer,
            offsets,
            current_offset,
        })
    }

    fn file(&mut self) -> &mut File {
        self.writer.get_mut()
    }
}

impl<C: Serialize> PersistentStorage<C> {
    async fn flush_writer(&mut self) -> Result<(), PersistentStorageError> {
        if !self.writer.write_buffer().is_empty() {
            self.file().seek(SeekFrom::End(0)).await?;
            self.writer.flush().await?;
        }
        Ok(())
    }
}

struct EntryEncoder<I> {
    phantom: PhantomData<I>,
}

impl<I> Default for EntryEncoder<I> {
    fn default() -> Self {
        Self {
            phantom: Default::default(),
        }
    }
}

const HEADER_SIZE: usize = std::mem::size_of::<u64>();

struct EncoderItem<'a, I> {
    inner: &'a I,
    size: u64,
}

impl<I> Encoder<EncoderItem<'_, I>> for EntryEncoder<I>
where
    I: Serialize,
{
    type Error = Box<bincode::ErrorKind>;

    fn encode(&mut self, item: EncoderItem<'_, I>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut writer = dst.writer();
        writer.write_u64::<LE>(item.size)?;
        bincode::DefaultOptions::new().serialize_into(writer, item.inner)?;
        Ok(())
    }
}

struct EntryDecoder<I> {
    inner: InnerDecoder,
    phantom: PhantomData<I>,
}

impl<I> Default for EntryDecoder<I> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            phantom: Default::default(),
        }
    }
}

impl<I> Decoder for EntryDecoder<I>
where
    for<'de> I: Deserialize<'de>,
{
    type Item = I;
    type Error = Box<bincode::ErrorKind>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner
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
