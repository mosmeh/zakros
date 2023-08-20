use crate::{
    rpc::{AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse, Transport},
    storage::{Storage, StorageExt},
    Command, Entry, EntryKind, Metadata, Node, NodeId, RaftConfig, RaftError, State, StateMachine,
    Status,
};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

pub struct Server<C, M, S, T>
where
    C: Command,
    T: Transport<Command = C>,
{
    node_id: NodeId,
    config: RaftConfig,

    state_machine: M,
    storage: S,
    transport: Arc<T>,

    // Persistent state on all servers:
    /// latest term server has seen
    current_term: u64,

    /// candidateId that received vote in current term
    voted_for: Option<NodeId>,

    // Volatile state on all servers:
    /// index of highest log entry known to be committed
    commit_index: u64,

    /// index of highest log entry applied to state machine
    last_applied_index: u64,

    last_applied_term: u64,
    last_message_index: u64,

    nodes: BTreeMap<NodeId, Node>,

    state: State,
    leader_id: Option<NodeId>,
    election_deadline: Instant,

    rx: mpsc::UnboundedReceiver<Message<C>>,

    pending_write_requests: VecDeque<WriteRequest<C::Output>>,
    pending_read_requests: VecDeque<ReadRequest>,

    pending_append_entries_responses:
        FuturesUnordered<JoinHandle<RpcResponse<AppendEntriesResponse, T::Error>>>,
    pending_request_vote_responses:
        FuturesUnordered<JoinHandle<RpcResponse<RequestVoteResponse, T::Error>>>,
}

impl<C, M, S, T> Server<C, M, S, T>
where
    C: Command,
    M: StateMachine<Command = C>,
    S: Storage<Command = C>,
    T: Transport<Command = C>,
{
    pub async fn new(
        id: NodeId,
        nodes: Vec<NodeId>,
        config: RaftConfig,
        state_machine: M,
        mut storage: S,
        transport: Arc<T>,
        rx: mpsc::UnboundedReceiver<Message<C>>,
    ) -> Self {
        let election_deadline = config.random_election_deadline();
        let Metadata {
            current_term,
            voted_for,
        } = storage.load().await.unwrap();
        tracing::info!("loaded {} entries", storage.num_entries());
        Self {
            node_id: id,
            config,
            state_machine,
            storage,
            transport,
            current_term,
            voted_for,
            commit_index: 0,
            last_applied_index: 0,
            last_applied_term: 0,
            last_message_index: 0,
            nodes: nodes.into_iter().map(|id| (id, Node::new())).collect(),
            state: State::Follower,
            leader_id: None,
            election_deadline,
            rx,
            pending_write_requests: Default::default(),
            pending_read_requests: Default::default(),
            pending_append_entries_responses: Default::default(),
            pending_request_vote_responses: Default::default(),
        }
    }

    pub async fn run(&mut self) {
        let mut heartbeat_timer = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        if self.nodes.len() == 1 {
            self.start_election().await;
        }

        loop {
            let election_timer = tokio::time::sleep_until(self.election_deadline);
            tokio::select! {
                _ = heartbeat_timer.tick(), if self.state == State::Leader => {
                    self.send_append_entries_to_all().await
                }
                _ = election_timer, if matches!(self.state, State::Follower | State::Candidate) => {
                    // If election timeout elapses: start new election
                    self.start_election().await
                }
                maybe_message = self.rx.recv() => match maybe_message {
                    Some(message) => self.handle_message(message).await,
                    None => break,
                },
                Some(response) = self.pending_append_entries_responses.next() => {
                    self.handle_append_entries_response(response.unwrap()).await
                }
                Some(response) = self.pending_request_vote_responses.next() => {
                    self.handle_request_vote_response(response.unwrap()).await
                }
            }
        }
    }

    async fn handle_message(&mut self, message: Message<C>) {
        match message {
            Message::AppendEntries(request, tx) => {
                let _ = tx.send(self.handle_append_entries(request).await);
            }
            Message::RequestVote(request, tx) => {
                let _ = tx.send(self.handle_request_vote(request).await);
            }
            Message::Write(command, tx) => self.handle_write(command, tx).await,
            Message::Read(tx) => self.handle_read(tx).await,
            Message::Status(tx) => {
                let _ = tx.send(Status {
                    state: self.state,
                    node_id: self.node_id,
                    leader_id: self.leader_id,
                });
            }
        }
    }

    async fn handle_append_entries(&mut self, request: AppendEntries<C>) -> AppendEntriesResponse {
        tracing::trace!("received AppendEntries");

        let AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            message_index,
        } = request;
        let current_index = self.storage.current_index();

        // 1. Reply false if term < currentTerm (S5.1)
        if term < self.current_term {
            tracing::trace!("replying to AppendEntries with false: term < currentTerm");
            return AppendEntriesResponse {
                term: self.current_term,
                current_index,
                success: false,
                message_index,
            };
        }

        if term > self.current_term {
            self.update_current_term(term).await;
        }

        self.reset_election_timer();
        if self.state != State::Follower {
            self.become_follower();
        }
        self.leader_id = Some(leader_id);

        if prev_log_index > 0 {
            // 2. Reply false if log doesn't contain an entry at prevLogIndex
            // whose term matches prevLogTerm (S5.3)
            match self.storage.entry(prev_log_index).await.unwrap() {
                Some(entry) if entry.term != prev_log_term => {
                    self.truncate_log(prev_log_index).await;
                    tracing::trace!(
                        "replying to AppendEntries with false: log[prevLogIndex].term != prevLogTerm"
                    );
                    return AppendEntriesResponse {
                        term: self.current_term,
                        current_index,
                        success: false,
                        message_index,
                    };
                }
                None => {
                    tracing::trace!("replying AppendEntries with false: no log[prevLogIndex]");
                    return AppendEntriesResponse {
                        term: self.current_term,
                        current_index,
                        success: false,
                        message_index,
                    };
                }
                _ => (),
            }
        }

        // 3. If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it (S5.3)
        let mut num_matching = 0;
        for (i, new_entry) in entries.iter().enumerate() {
            let index_in_log = prev_log_index + TryInto::<u64>::try_into(i).unwrap() + 1;
            let Some(existing_entry) = self.storage.entry(index_in_log).await.unwrap() else {
                break;
            };
            if existing_entry.term != new_entry.term {
                self.truncate_log(index_in_log).await;
                break;
            }
            num_matching += 1;
        }

        // 4. Append any new entries not already in the log
        if num_matching < entries.len() {
            self.storage
                .append_entries(&entries[num_matching..])
                .await
                .unwrap();
        }

        // 5. If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if leader_commit > self.commit_index {
            self.commit_index = self.storage.current_index().clamp(1, leader_commit);
        }

        self.exec_operations().await;

        tracing::trace!("acknowledging AppendEntries");
        AppendEntriesResponse {
            term: self.current_term,
            current_index: prev_log_index + TryInto::<u64>::try_into(entries.len()).unwrap(),
            success: true,
            message_index,
        }
    }

    async fn handle_append_entries_response(
        &mut self,
        response: RpcResponse<AppendEntriesResponse, T::Error>,
    ) {
        let RpcResponse { node_id, result } = response;
        let response = match result {
            Ok(response) => response,
            Err(err) => {
                tracing::trace!("AppendEntries request to {:?} failed: {:?}", node_id, err);
                return;
            }
        };
        tracing::trace!("received AppendEntries reply from {:?}", node_id);

        if self.state != State::Leader {
            return;
        }

        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (S5.1)
        if response.term > self.current_term {
            self.update_current_term(response.term).await;
            self.become_follower();
            return;
        }

        let node = self.nodes.get_mut(&node_id).unwrap();
        if !response.success {
            if response.current_index < node.match_index {
                return;
            }
            node.next_index = self
                .storage
                .current_index()
                .clamp(1, response.current_index + 1);
            let fut = self.spawn_append_entries_task(node_id).await;
            self.pending_append_entries_responses.push(fut);
            return;
        }

        // If successful: update nextIndex and matchIndex for
        // follower (S5.3)
        node.next_index = self.storage.current_index().max(1);
        node.match_index = node.match_index.max(response.current_index);
        node.match_message_index = node.match_message_index.max(response.message_index);

        self.flush().await;
    }

    async fn handle_request_vote(&mut self, request: RequestVote) -> RequestVoteResponse {
        tracing::trace!("received RequestVote");

        let RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } = request;

        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (S5.1)
        if term > self.current_term {
            self.update_current_term(term).await;
            self.become_follower();
        }

        // 1. Reply false if term < currentTerm (S5.1)
        if term < self.current_term {
            tracing::trace!("rejecting RequestVote: term < currentTerm");
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        // 2. If votedFor is null or candidateId, and candidate's log is at
        // least as up-to-date as receiver's log, grant vote (S5.2, S5.4)
        match self.voted_for {
            None => (),
            Some(node_id) if node_id == candidate_id => (),
            Some(node_id) => {
                tracing::trace!("rejecting RequestVote: already voted for {:?}", node_id);
                return RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                };
            }
        }

        let current_index = self.storage.current_index();
        let last_term = self.storage.last_term().await.unwrap();
        if (last_log_term, last_log_index) < (last_term, current_index) {
            tracing::trace!("rejecting RequestVote: last log is too old",);
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        assert_eq!(self.state, State::Follower);
        self.vote_for(candidate_id).await;
        self.reset_election_timer();

        tracing::trace!("voting to {:?}", candidate_id);
        RequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        }
    }

    async fn handle_request_vote_response(
        &mut self,
        response: RpcResponse<RequestVoteResponse, T::Error>,
    ) {
        let RpcResponse { node_id, result } = response;
        let response = match result {
            Ok(response) => response,
            Err(err) => {
                tracing::trace!("RequestVote request to {:?} failed: {:?}", node_id, err);
                return;
            }
        };
        tracing::trace!("received RequestVote response from {:?}", node_id);

        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (S5.1)
        if response.term > self.current_term {
            self.update_current_term(response.term).await;
            self.become_follower();
            return;
        }

        if self.state != State::Candidate || response.term != self.current_term {
            return;
        }

        // If votes received from majority of servers: become leader
        if response.vote_granted {
            self.nodes.get_mut(&node_id).unwrap().voted_for_me = true;
            if self.nodes.values().filter(|node| node.voted_for_me).count() > self.nodes.len() / 2 {
                self.become_leader().await;
            }
        }
    }

    async fn handle_write(
        &mut self,
        command: C,
        tx: oneshot::Sender<Result<C::Output, RaftError>>,
    ) {
        if self.state != State::Leader {
            tracing::trace!("rejecting write request");
            let _ = tx.send(Err(RaftError::NotLeader {
                leader_id: self.leader_id,
            }));
            return;
        }

        // If command received from client: append entry to local log,
        // respond after entry applied to state machine (S5.3)
        self.storage
            .append_entries(&[Entry {
                kind: EntryKind::Command(command),
                term: self.current_term,
            }])
            .await
            .unwrap();

        let index = self.storage.current_index();
        self.nodes.get_mut(&self.node_id).unwrap().match_index = index;

        tracing::trace!(index, "pending write request",);
        self.pending_write_requests
            .push_back(WriteRequest { index, tx });

        self.flush().await;
    }

    async fn handle_read(&mut self, tx: oneshot::Sender<Result<(), RaftError>>) {
        if self.state == State::Leader {
            let index = self.storage.current_index();
            tracing::trace!(index, "pending read request",);
            self.last_message_index += 1;
            self.pending_read_requests.push_back(ReadRequest {
                index,
                message_index: self.last_message_index,
                tx,
            });
            self.flush().await;
        } else {
            tracing::trace!("rejecting read request");
            let _ = tx.send(Err(RaftError::NotLeader {
                leader_id: self.leader_id,
            }));
        }
    }

    async fn flush(&mut self) {
        assert_eq!(self.state, State::Leader);
        self.update_commit_index().await;
        self.exec_operations().await;
    }

    async fn truncate_log(&mut self, index: u64) {
        assert_eq!(self.state, State::Follower);
        self.storage.truncate_entries(index).await.unwrap();
        while let Some(request) = self.pending_write_requests.back() {
            if request.index < index {
                break;
            }
            let request = self.pending_write_requests.pop_back().unwrap();
            let _ = request.tx.send(Err(RaftError::NotLeader {
                leader_id: self.leader_id,
            }));
        }
    }

    async fn send_append_entries_to_all(&mut self) {
        assert_eq!(self.state, State::Leader);

        let node_ids: Vec<_> = self
            .nodes
            .keys()
            .filter(|node_id| **node_id != self.node_id)
            .copied()
            .collect();
        for node_id in node_ids {
            // If last log index ≥ nextIndex for a follower: send
            // AppendEntries RPC with log entries starting at nextIndex
            let fut = self.spawn_append_entries_task(node_id).await;
            self.pending_append_entries_responses.push(fut);
        }
    }

    async fn exec_operations(&mut self) {
        // If commitIndex > lastApplied: increment lastApplied, apply
        // log[lastApplied] to state machine (S5.3)
        tracing::trace!(
            commit_index = self.commit_index,
            last_applied_index = self.last_applied_index,
            "applying {} entries",
            self.commit_index - self.last_applied_index
        );
        while self.commit_index > self.last_applied_index {
            let next_applied_index = self.last_applied_index + 1;
            let entry = self
                .storage
                .entry(next_applied_index)
                .await
                .unwrap()
                .unwrap();
            match entry.kind {
                EntryKind::NoOp => (),
                EntryKind::Command(command) => {
                    let output = self.state_machine.apply(command).await;
                    if let Some(request) = self.pending_write_requests.front() {
                        assert!(request.index <= next_applied_index);
                        if request.index == next_applied_index {
                            let request = self.pending_write_requests.pop_front().unwrap();
                            tracing::trace!("acknowledging write request");
                            let _ = request.tx.send(Ok(output));
                        }
                    }
                }
                EntryKind::AddNode { .. } => todo!(),
                EntryKind::RemoveNode(_) => todo!(),
            }
            self.last_applied_index = next_applied_index;
            self.last_applied_term = entry.term;
        }

        if self.state != State::Leader {
            for request in self.pending_read_requests.drain(..) {
                let _ = request.tx.send(Err(RaftError::NotLeader {
                    leader_id: self.leader_id,
                }));
            }
            return;
        }

        if self.last_applied_term < self.current_term {
            return;
        }

        let mut match_message_indices: Vec<_> = self
            .nodes
            .iter()
            .map(|(node_id, node)| {
                if *node_id == self.node_id {
                    self.last_message_index
                } else {
                    node.match_message_index
                }
            })
            .collect();
        let middle = match_message_indices.len() / 2;
        let (_, &mut quorum_message_index, _) = match_message_indices.select_nth_unstable(middle);

        while let Some(request) = self.pending_read_requests.front() {
            if request.message_index > quorum_message_index
                || request.index > self.last_applied_index
            {
                break;
            }
            let request = self.pending_read_requests.pop_front().unwrap();
            tracing::trace!("acknowledging read request");
            let _ = request.tx.send(Ok(()));
        }
    }

    async fn update_current_term(&mut self, new_term: u64) {
        assert!(new_term > self.current_term);
        self.storage
            .persist_metadata(&Metadata {
                current_term: new_term,
                voted_for: None,
            })
            .await
            .unwrap();
        self.current_term = new_term;
        self.voted_for = None;
    }

    async fn vote_for(&mut self, node_id: NodeId) {
        self.storage
            .persist_metadata(&Metadata {
                current_term: self.current_term,
                voted_for: Some(node_id),
            })
            .await
            .unwrap();
        self.voted_for = Some(node_id);
    }

    async fn update_commit_index(&mut self) {
        assert_eq!(self.state, State::Leader);

        // If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] >= N, and log[N].term == currentTerm:
        // set commitIndex = N (S5.3, S5.4).
        let mut match_indices: Vec<_> = self.nodes.values().map(|node| node.match_index).collect();
        let i = match_indices.len() / 2;
        let (_, &mut n, _) = match_indices.select_nth_unstable(i);
        if n > self.commit_index {
            let entry = self.storage.entry(n).await.unwrap().unwrap();
            if entry.term == self.current_term {
                assert!(n <= self.storage.current_index());
                self.commit_index = n;
            }
        }
    }

    fn reset_election_timer(&mut self) {
        self.election_deadline = self.config.random_election_deadline();
    }

    async fn start_election(&mut self) {
        assert!(matches!(self.state, State::Follower | State::Candidate));
        tracing::info!("start election");
        if self.nodes.len() == 1 {
            assert!(self.nodes.contains_key(&self.node_id));
            self.update_current_term(self.current_term + 1).await;
            self.become_leader().await;
        } else {
            self.become_candidate().await;
        }
    }

    fn become_follower(&mut self) {
        tracing::info!(term = self.current_term, "became follower");
        self.state = State::Follower;
        self.leader_id = None;
        self.reset_election_timer();
    }

    async fn become_candidate(&mut self) {
        tracing::info!(term = self.current_term, "became candidate");
        self.state = State::Candidate;

        // On conversion to candidate, start election:

        // Increment currentTerm
        self.update_current_term(self.current_term + 1).await;

        // Vote for self
        self.vote_for(self.node_id).await;

        // Reset election timer
        self.reset_election_timer();

        self.leader_id = None;
        for (node_id, node) in self.nodes.iter_mut() {
            node.voted_for_me = *node_id == self.node_id;
        }

        // Send RequestVote RPCs to all other servers
        let last_log_term = self.storage.last_term().await.unwrap();
        let last_log_index = self.storage.current_index();
        let request = RequestVote {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_index,
            last_log_term,
        };
        for &node_id in self
            .nodes
            .keys()
            .filter(|node_id| **node_id != self.node_id)
        {
            let transport = self.transport.clone();
            let request = request.clone();
            tracing::trace!("sending RequestVote to {:?}", node_id);
            self.pending_request_vote_responses
                .push(tokio::spawn(async move {
                    RpcResponse {
                        node_id,
                        result: transport.send_request_vote(node_id, request).await,
                    }
                }));
        }
    }

    async fn become_leader(&mut self) {
        tracing::info!(term = self.current_term, "became leader");
        self.state = State::Leader;
        self.leader_id = Some(self.node_id);

        self.storage
            .append_entries(&[Entry {
                kind: EntryKind::NoOp,
                term: self.current_term,
            }])
            .await
            .unwrap();

        let current_index = self.storage.current_index();
        for (node_id, node) in self.nodes.iter_mut() {
            if *node_id == self.node_id {
                node.match_index = current_index;
            } else {
                node.next_index = current_index.max(1);
                node.match_index = 0;
            }
        }

        // Upon election: send initial empty AppendEntries RPCs
        // (heartbeat) to each server
        self.send_append_entries_to_all().await;

        self.flush().await;
    }

    async fn spawn_append_entries_task(
        &mut self,
        dest: NodeId,
    ) -> JoinHandle<RpcResponse<AppendEntriesResponse, T::Error>> {
        let node = self.nodes.get(&dest).unwrap();
        let prev_log_term = self
            .storage
            .entry(node.next_index - 1)
            .await
            .unwrap()
            .map(|entry| entry.term)
            .unwrap_or(0);
        let entries = self.storage.entries(node.next_index).await.unwrap();
        let num_entries = entries.len();
        self.last_message_index += 1;
        let request = AppendEntries {
            term: self.current_term,
            leader_id: self.node_id,
            prev_log_index: node.next_index - 1,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
            message_index: self.last_message_index,
        };
        let transport = self.transport.clone();
        tracing::trace!(
            "sending AppendEntries with {} entries to {:?}",
            num_entries,
            dest
        );
        tokio::spawn(async move {
            RpcResponse {
                node_id: dest,
                result: transport.send_append_entries(dest, request).await,
            }
        })
    }
}

pub enum Message<C: Command> {
    AppendEntries(AppendEntries<C>, oneshot::Sender<AppendEntriesResponse>),
    RequestVote(RequestVote, oneshot::Sender<RequestVoteResponse>),
    Write(C, oneshot::Sender<Result<C::Output, RaftError>>),
    Read(oneshot::Sender<Result<(), RaftError>>),
    Status(oneshot::Sender<Status>),
}

struct RpcResponse<R, E> {
    node_id: NodeId,
    result: Result<R, E>,
}

#[derive(Debug)]
struct WriteRequest<O> {
    index: u64,
    tx: oneshot::Sender<Result<O, RaftError>>,
}

#[derive(Debug)]
struct ReadRequest {
    index: u64,
    message_index: u64,
    tx: oneshot::Sender<Result<(), RaftError>>,
}
