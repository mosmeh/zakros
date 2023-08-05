use bytes::Bytes;
use futures::{Stream, StreamExt};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    pin::Pin,
    sync::Arc,
    task::Poll,
};
use tokio::sync::broadcast::{self, error::RecvError, Receiver, Sender};

pub struct Publisher(Arc<RwLock<Shared>>);

impl Publisher {
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(RwLock::new(Shared {
            capacity,
            channels: Default::default(),
            patterns: Default::default(),
        })))
    }

    pub fn publish(&self, message: PubSubMessage) -> usize {
        self.0.read().publish(message)
    }

    pub fn subscriber(&self) -> Subscriber {
        Subscriber {
            shared: self.0.clone(),
            channels: Default::default(),
            patterns: Default::default(),
        }
    }

    pub fn active_channels(&self) -> Vec<Bytes> {
        self.0.read().channels.keys().cloned().collect()
    }

    pub fn active_channels_matching_pattern(&self, pattern: impl AsRef<[u8]>) -> Vec<Bytes> {
        let pattern = pattern.as_ref();
        self.0
            .read()
            .channels
            .keys()
            .filter(|channel| crate::string_match(pattern, channel))
            .cloned()
            .collect()
    }

    pub fn num_patterns(&self) -> usize {
        self.0.read().patterns.len()
    }

    pub fn num_subscribers(&self, channel: &Bytes) -> usize {
        match self.0.read().channels.get(channel) {
            Some(tx) => tx.receiver_count(),
            None => 0,
        }
    }
}

#[derive(Default)]
struct Shared {
    capacity: usize,
    channels: HashMap<Bytes, Sender<Bytes>>,
    patterns: HashMap<Bytes, Sender<PubSubMessage>>,
}

impl Shared {
    fn publish(&self, message: PubSubMessage) -> usize {
        let PubSubMessage { channel, payload } = message;
        let mut num_receivers = 0;
        if let Some(tx) = self.channels.get(&channel) {
            num_receivers += tx.send(payload.clone()).unwrap()
        }
        for (pattern, tx) in &self.patterns {
            if crate::string_match(pattern, &channel) {
                num_receivers += tx
                    .send(PubSubMessage {
                        channel: channel.clone(),
                        payload: payload.clone(),
                    })
                    .unwrap();
            }
        }
        num_receivers
    }

    fn subscribe_to_channel(&mut self, channel: Bytes) -> Receiver<Bytes> {
        match self.channels.entry(channel) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                let (tx, rx) = broadcast::channel(self.capacity);
                entry.insert(tx);
                rx
            }
        }
    }

    fn unsubscribe_from_channel(&mut self, channel: Bytes) -> bool {
        match self.channels.entry(channel) {
            Entry::Occupied(entry) => {
                let num_receivers = entry.get().receiver_count();
                assert!(num_receivers > 0);
                if num_receivers == 1 {
                    entry.remove();
                }
                true
            }
            Entry::Vacant(_) => false,
        }
    }

    fn subscribe_to_pattern(&mut self, pattern: Bytes) -> Receiver<PubSubMessage> {
        match self.patterns.entry(pattern) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                let (tx, rx) = broadcast::channel(self.capacity);
                entry.insert(tx);
                rx
            }
        }
    }

    fn unsubscribe_from_pattern(&mut self, pattern: Bytes) -> bool {
        match self.patterns.entry(pattern) {
            Entry::Occupied(entry) => {
                let num_receivers = entry.get().receiver_count();
                assert!(num_receivers > 0);
                if num_receivers == 1 {
                    entry.remove();
                }
                true
            }
            Entry::Vacant(_) => false,
        }
    }
}

type ReceiverStream<T> = Pin<Box<dyn Stream<Item = Result<T, SubscriberRecvError>> + Send + Sync>>;

pub struct Subscriber {
    shared: Arc<RwLock<Shared>>,
    channels: HashMap<Bytes, ReceiverStream<Bytes>>,
    patterns: HashMap<Bytes, ReceiverStream<PubSubMessage>>,
}

impl Subscriber {
    pub fn num_subscriptions(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }

    pub fn subscribe_to_channel(&mut self, channel: Bytes) -> bool {
        match self.channels.entry(channel.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                let mut rx = self.shared.write().subscribe_to_channel(channel);
                entry.insert(Box::pin(async_stream::stream! {
                    loop {
                        match rx.recv().await {
                            Ok(payload) => yield Ok(payload),
                            Err(RecvError::Closed) => return,
                            Err(RecvError::Lagged(_)) => yield Err(SubscriberRecvError::Lagged),
                        }
                    }
                }));
                true
            }
        }
    }

    pub fn unsubscribe_from_channel(&mut self, channel: Bytes) -> bool {
        if self.channels.remove(&channel).is_some() {
            assert!(self.shared.write().unsubscribe_from_channel(channel));
            true
        } else {
            false
        }
    }

    pub fn subscribe_to_pattern(&mut self, pattern: Bytes) -> bool {
        match self.patterns.entry(pattern.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                let mut rx = self.shared.write().subscribe_to_pattern(pattern);
                entry.insert(Box::pin(async_stream::stream! {
                    loop {
                        match rx.recv().await {
                            Ok(message) => yield Ok(message),
                            Err(RecvError::Closed) => return,
                            Err(RecvError::Lagged(_)) => yield Err(SubscriberRecvError::Lagged),
                        }
                    }
                }));
                true
            }
        }
    }

    pub fn unsubscribe_from_pattern(&mut self, pattern: Bytes) -> bool {
        if self.patterns.remove(&pattern).is_some() {
            assert!(self.shared.write().unsubscribe_from_pattern(pattern));
            true
        } else {
            false
        }
    }
}

impl Stream for Subscriber {
    type Item = Result<PubSubMessage, SubscriberRecvError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        for (channel, rx) in &mut self.channels {
            match rx.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(payload))) => {
                    return Poll::Ready(Some(Ok(PubSubMessage {
                        channel: channel.clone(),
                        payload,
                    })))
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => unreachable!(),
                Poll::Pending => (),
            }
        }
        for rx in self.patterns.values_mut() {
            match rx.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(message))) => return Poll::Ready(Some(Ok(message))),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => unreachable!(),
                Poll::Pending => (),
            }
        }
        Poll::Pending
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubscriberRecvError {
    #[error("The subscriber lagged too far behind")]
    Lagged,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let mut shared = self.shared.write();
        for (channel, _) in self.channels.drain() {
            assert!(shared.unsubscribe_from_channel(channel));
        }
        for (pattern, _) in self.patterns.drain() {
            assert!(shared.unsubscribe_from_pattern(pattern));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub channel: Bytes,
    pub payload: Bytes,
}
