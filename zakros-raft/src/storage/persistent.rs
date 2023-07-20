use super::Storage;
use crate::{Entry, Metadata};
use async_trait::async_trait;
use bincode::Options;
use byteorder::{NativeEndian, WriteBytesExt};
use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};
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
    dir: PathBuf,
    writer: FramedWrite<File, EntryEncoder<Entry<C>>>,
    offsets: Vec<u64>,
    current_offset: u64,
}

#[async_trait]
impl<C> Storage for PersistentStorage<C>
where
    for<'de> C: Serialize + Deserialize<'de> + Clone + Send + Sync + std::fmt::Debug + 'static,
{
    type Command = C;
    type Error = PersistentStorageError;

    async fn load(&mut self) -> Result<Metadata, Self::Error> {
        match File::open(self.dir.join("metadata")).await {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).await?;
                Ok(bincode_options().deserialize(&bytes).unwrap())
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
        self.file_mut()
            .seek(std::io::SeekFrom::Start(offset))
            .await?;
        let mut reader = FramedRead::new(self.file_mut(), EntryDecoder::default());
        Ok(match reader.next().await {
            Some(entry) => Some(entry?),
            None => None,
        })
    }

    async fn entries(&mut self, start: u64) -> Result<Vec<Entry<Self::Command>>, Self::Error> {
        assert!(start > 0);
        let index: usize = (start - 1).try_into().unwrap();
        let Some(&offset) = self.offsets.get(index) else {
            return Ok(Vec::new());
        };
        self.file_mut()
            .seek(std::io::SeekFrom::Start(offset))
            .await?;
        let mut reader = FramedRead::new(self.file_mut(), EntryDecoder::default());
        let mut entries = Vec::new();
        while let Some(entry) = reader.next().await {
            entries.push(entry.unwrap());
        }
        Ok(entries)
    }

    async fn append_entries(
        &mut self,
        entries: &[Entry<Self::Command>],
    ) -> Result<(), Self::Error> {
        self.file_mut().seek(std::io::SeekFrom::End(0)).await?;
        for entry in entries {
            let size = bincode_options().serialized_size(entry)?;
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
        self.file().set_len(offset).await?;
        self.offsets.truncate(index);
        self.persist_entries().await?;
        Ok(())
    }

    async fn persist_metadata(&mut self, metadata: &Metadata) -> Result<(), Self::Error> {
        let bytes = bincode_options().serialize(metadata)?;
        let tmp_filename = self.dir.join("metadata.tmp");
        {
            let mut tmp_file = File::create(&tmp_filename).await?;
            tmp_file.write_all(&bytes).await?;
            tmp_file.sync_data().await?;
        }
        tokio::fs::rename(tmp_filename, self.dir.join("metadata")).await?;
        // TODO: fsync directory
        Ok(())
    }

    async fn persist_entries(&mut self) -> Result<(), Self::Error> {
        self.file_mut().seek(std::io::SeekFrom::End(0)).await?;
        self.writer.flush().await?;
        self.file().sync_data().await?;
        Ok(())
    }
}

impl<C> PersistentStorage<C> {
    pub async fn new(dir: impl AsRef<Path>) -> Result<Self, PersistentStorageError> {
        let dir = dir.as_ref().to_owned();
        tokio::fs::create_dir_all(&dir).await?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(dir.join("log"))
            .await?;
        let mut reader = FramedRead::new(&mut file, SizeDecoder::default());
        let mut offsets = Vec::new();
        let mut current_offset = 0;
        while let Some(offset) = reader.next().await {
            offsets.push(current_offset);
            current_offset += offset.unwrap() + HEADER_SIZE as u64;
        }
        let writer = FramedWrite::new(file, EntryEncoder::default());
        Ok(Self {
            dir,
            writer,
            offsets,
            current_offset,
        })
    }

    fn file(&self) -> &File {
        self.writer.get_ref()
    }

    fn file_mut(&mut self) -> &mut File {
        self.writer.get_mut()
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
        writer.write_u64::<NativeEndian>(item.size).unwrap();
        bincode_options()
            .serialize_into(writer, item.inner)
            .unwrap();
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
            .decode(src, |s| bincode_options().deserialize_from(s))
    }
}

fn bincode_options() -> impl Options {
    bincode::DefaultOptions::default().with_native_endian()
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
                src.get_u64_ne() as usize
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
