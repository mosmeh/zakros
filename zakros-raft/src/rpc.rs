use crate::{Command, Entry, NodeId};
use serde::{Deserialize, Serialize};

pub trait Transport: Send + Sync + 'static {
    type Command: Command;
    type Error: Send + std::fmt::Debug;

    fn send_append_entries(
        &self,
        dest: NodeId,
        request: AppendEntries<Self::Command>,
    ) -> impl std::future::Future<Output = Result<AppendEntriesResponse, Self::Error>> + std::marker::Send;

    fn send_request_vote(
        &self,
        dest: NodeId,
        request: RequestVote,
    ) -> impl std::future::Future<Output = Result<RequestVoteResponse, Self::Error>> + std::marker::Send;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries<C> {
    /// leader's term
    pub(crate) term: u64,

    pub(crate) leader_id: NodeId,

    /// index of log entry immediately preceding new ones
    pub(crate) prev_log_index: u64,

    /// term of prevLogIndex entry
    pub(crate) prev_log_term: u64,

    /// log entries to store
    pub(crate) entries: Vec<Entry<C>>,

    /// leader's commitIndex
    pub(crate) leader_commit: u64,

    pub(crate) message_index: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// currentTerm, for leader to update itself
    pub(crate) term: u64,

    /// true if follower contained entry matching prevLogIndex and prevLogTerm
    pub(crate) success: bool,

    pub(crate) message_index: u64,
    pub(crate) current_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    /// candidate's term
    pub(crate) term: u64,

    /// candidate requesting vote
    pub(crate) candidate_id: NodeId,

    /// index of candidate's last log entry
    pub(crate) last_log_index: u64,

    /// term of candidate's last log entry
    pub(crate) last_log_term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// currentTerm, for candidate to update itself
    pub(crate) term: u64,

    /// true means candidate received vote
    pub(crate) vote_granted: bool,
}
