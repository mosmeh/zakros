pub mod storage;
pub mod transport;

mod server;

use async_trait::async_trait;
use rand::{distributions::Uniform, prelude::Distribution};
use serde::{Deserialize, Serialize};
use server::{Message, Server};
use std::{fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};
use storage::Storage;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use transport::{
    AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse, Transport,
};

#[derive(Clone)]
pub struct Raft<C: Command> {
    tx: mpsc::UnboundedSender<Message<C>>,
}

impl<C: Command> Raft<C> {
    pub fn new<M, S, T>(
        id: NodeId,
        nodes: Vec<NodeId>,
        config: Config,
        state_machine: M,
        storage: S,
        transport: Arc<T>,
    ) -> Self
    where
        M: StateMachine<Command = C>,
        S: Storage<Command = C>,
        T: Transport<Command = C>,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            Server::new(id, nodes, config, state_machine, storage, transport, rx)
                .await
                .run()
                .await;
        });
        Self { tx }
    }

    pub async fn write(&self, command: C) -> Result<C::Output, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::Write(command, tx))
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)?
    }

    pub async fn read(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::Read(tx))
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)?
    }

    pub async fn append_entries(
        &self,
        request: AppendEntries<C>,
    ) -> Result<AppendEntriesResponse, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::AppendEntries(request, tx))
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    pub async fn request_vote(&self, request: RequestVote) -> Result<RequestVoteResponse, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::RequestVote(request, tx))
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    pub async fn status(&self) -> Result<Status, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::Status(tx))
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }
}

pub struct Config {
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(200),
            election_timeout_min: Duration::from_secs(1),
            election_timeout_max: Duration::from_secs(2),
        }
    }
}

impl Config {
    fn random_election_deadline(&self) -> Instant {
        let dist = Uniform::new(self.election_timeout_min, self.election_timeout_max);
        tokio::time::Instant::now() + dist.sample(&mut rand::thread_rng())
    }
}

#[derive(Clone)]
pub struct Status {
    pub state: State,
    pub node_id: NodeId,
    pub leader_id: Option<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(u64);

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<NodeId> for u64 {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

struct Node {
    // Volatile state on leaders:
    /// index of the next log entry to send to that server
    next_index: u64,

    /// index of highest log entry known to be replicated on server
    match_index: u64,

    match_message_index: u64,
    voted_for_me: bool,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            next_index: 1,
            match_index: 0,
            match_message_index: 0,
            voted_for_me: false,
        }
    }
}

impl Node {
    fn new() -> Self {
        Default::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry<C> {
    kind: EntryKind<C>,
    term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EntryKind<C> {
    NoOp,
    Command(C),
    AddNode { node_id: NodeId, addr: SocketAddr },
    RemoveNode(NodeId),
}

#[derive(Default, Serialize, Deserialize)]
pub struct Metadata {
    /// latest term server has seen
    current_term: u64,

    /// candidateId that received vote in current term
    voted_for: Option<NodeId>,
}

#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    #[error("Raft server is shut down")]
    Shutdown,

    #[error("This is not a leader node")]
    NotLeader { leader_id: Option<NodeId> },
}

pub trait Command: Send + Sync + Clone + 'static {
    type Output: Send;
}

#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    type Command: Command;

    async fn apply(&mut self, command: Self::Command) -> <Self::Command as Command>::Output;
}
