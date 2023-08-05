use super::{Entry, Metadata, Storage};
use crate::Command;
use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum MemoryStorageError {
    #[error("Index is too large")]
    IndexTooLarge,
}

pub struct MemoryStorage<C>(Vec<Entry<C>>);

impl<C> MemoryStorage<C> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<C> Default for MemoryStorage<C> {
    fn default() -> Self {
        Self(Default::default())
    }
}

#[async_trait]
impl<C: Command> Storage for MemoryStorage<C> {
    type Command = C;
    type Error = MemoryStorageError;

    async fn load(&mut self) -> Result<Metadata, Self::Error> {
        Ok(Metadata::default())
    }

    fn num_entries(&self) -> usize {
        self.0.len()
    }

    async fn entry(&mut self, index: u64) -> Result<Option<Entry<Self::Command>>, Self::Error> {
        Ok(if index > 0 {
            let index = TryInto::<usize>::try_into(index)
                .map_err(|_| MemoryStorageError::IndexTooLarge)?
                - 1;
            self.0.get(index).cloned()
        } else {
            None
        })
    }

    async fn entries(&mut self, start: u64) -> Result<Vec<Entry<Self::Command>>, Self::Error> {
        assert!(start > 0);
        let start =
            TryInto::<usize>::try_into(start).map_err(|_| MemoryStorageError::IndexTooLarge)? - 1;
        if start >= self.0.len() {
            return Ok(Vec::new());
        }
        Ok(self.0[start..].to_vec())
    }

    async fn append_entries(
        &mut self,
        entries: &[Entry<Self::Command>],
    ) -> Result<(), Self::Error> {
        self.0.extend_from_slice(entries);
        Ok(())
    }

    async fn truncate_entries(&mut self, index: u64) -> Result<(), Self::Error> {
        assert!(index > 0);
        let index =
            TryInto::<usize>::try_into(index).map_err(|_| MemoryStorageError::IndexTooLarge)? - 1;
        self.0.truncate(index);
        Ok(())
    }

    async fn persist_metadata(&mut self, _metadata: &Metadata) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn persist_entries(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
