use super::{Entry, Metadata, Storage};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::{collections::VecDeque, convert::Infallible};
pub struct VolatileStorage<C>(RwLock<VecDeque<Entry<C>>>);

impl<C> Default for VolatileStorage<C> {
    fn default() -> Self {
        Self(Default::default())
    }
}

#[async_trait]
impl<C> Storage for VolatileStorage<C>
where
    C: Clone + Send + Sync + 'static,
{
    type Command = C;
    type Error = Infallible;

    async fn load(&mut self) -> Result<Metadata, Self::Error> {
        Ok(Metadata::default())
    }

    fn num_entries(&self) -> usize {
        self.0.read().len()
    }

    async fn entry(&mut self, index: u64) -> Result<Option<Entry<Self::Command>>, Self::Error> {
        Ok(if index > 0 {
            self.0
                .read()
                .get(TryInto::<usize>::try_into(index).unwrap() - 1)
                .map(Clone::clone)
        } else {
            None
        })
    }

    async fn entries(&mut self, start: u64) -> Result<Vec<Entry<Self::Command>>, Self::Error> {
        assert!(start > 0);
        Ok(self
            .0
            .read()
            .iter()
            .skip(TryInto::<usize>::try_into(start).unwrap() - 1)
            .cloned()
            .collect())
    }

    async fn append_entries(
        &mut self,
        entries: &[Entry<Self::Command>],
    ) -> Result<(), Self::Error> {
        self.0.write().extend(entries.iter().cloned());
        Ok(())
    }

    async fn truncate_entries(&mut self, index: u64) -> Result<(), Self::Error> {
        assert!(index > 0);
        self.0
            .write()
            .drain((TryInto::<usize>::try_into(index).unwrap() - 1)..);
        Ok(())
    }

    async fn persist_metadata(&mut self, _metadata: &Metadata) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn persist_entries(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
