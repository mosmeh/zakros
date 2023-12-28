mod disk;
mod memory;

pub use disk::{DiskStorage, DiskStorageError};
pub use memory::MemoryStorage;

use super::{Entry, Metadata};
use crate::Command;
use futures::Future;

// TODO: support log compaction

pub trait Storage: Send + Sync + 'static {
    type Command: Command;
    type Error: Send + std::fmt::Debug;

    fn load(&mut self) -> impl Future<Output = Result<Metadata, Self::Error>> + Send;

    fn num_entries(&self) -> usize;
    fn entry(
        &mut self,
        index: u64,
    ) -> impl Future<Output = Result<Option<Entry<Self::Command>>, Self::Error>> + Send;
    fn entries(
        &mut self,
        start: u64,
    ) -> impl Future<Output = Result<Vec<Entry<Self::Command>>, Self::Error>> + Send;
    fn append_entries(
        &mut self,
        entries: &[Entry<Self::Command>],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn truncate_entries(
        &mut self,
        index: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn persist_metadata(
        &mut self,
        metadata: &Metadata,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn persist_entries(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub(crate) trait StorageExt: Storage {
    fn current_index(&self) -> u64 {
        self.num_entries().try_into().unwrap()
    }

    async fn last_term(&mut self) -> Result<u64, <Self as Storage>::Error> {
        let index = self.current_index();
        Ok(if index > 0 {
            self.entry(index)
                .await?
                .map(|entry| entry.term)
                .unwrap_or(0)
        } else {
            0
        })
    }
}

impl<S: Storage> StorageExt for S {}
