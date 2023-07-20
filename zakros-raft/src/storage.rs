mod persistent;
mod volatile;

pub use persistent::{PersistentStorage, PersistentStorageError};
pub use volatile::VolatileStorage;

use super::{Entry, Metadata};
use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync + 'static {
    type Command: Send + Sync;
    type Error: Send + std::fmt::Debug;

    async fn load(&mut self) -> Result<Metadata, Self::Error>;

    fn num_entries(&self) -> usize;
    async fn entry(&mut self, index: u64) -> Result<Option<Entry<Self::Command>>, Self::Error>;
    async fn entries(&mut self, start: u64) -> Result<Vec<Entry<Self::Command>>, Self::Error>;
    async fn append_entries(&mut self, entries: &[Entry<Self::Command>])
        -> Result<(), Self::Error>;
    async fn truncate_entries(&mut self, index: u64) -> Result<(), Self::Error>;

    async fn persist_metadata(&mut self, metadata: &Metadata) -> Result<(), Self::Error>;
    async fn persist_entries(&mut self) -> Result<(), Self::Error>;
}

#[async_trait]
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
