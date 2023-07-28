use crate::{store::Command, Opts, RaftResult, SharedState};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tarpc::{context::Context, tokio_serde::formats::Bincode};
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};
use tokio_util::codec::LengthDelimitedCodec;
use zakros_raft::{
    self,
    transport::{
        AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse, Transport,
    },
    NodeId,
};

pub struct RpcHandler(pub(crate) Opts);

#[async_trait]
impl Transport for RpcHandler {
    type Command = Command;
    type Error = anyhow::Error;

    async fn send_append_entries(
        &self,
        dest: NodeId,
        request: AppendEntries<Self::Command>,
    ) -> Result<AppendEntriesResponse, Self::Error> {
        timeout(Duration::from_secs(1), async {
            self.client(dest)
                .await?
                .append_entries(Context::current(), request)
                .await?
                .map_err(Into::into)
        })
        .await?
    }

    async fn send_request_vote(
        &self,
        dest: NodeId,
        request: RequestVote,
    ) -> Result<RequestVoteResponse, Self::Error> {
        timeout(Duration::from_secs(1), async {
            self.client(dest)
                .await?
                .request_vote(Context::current(), request)
                .await?
                .map_err(Into::into)
        })
        .await?
    }
}

impl RpcHandler {
    pub const RAFT_MARKER: &[u8] = b"\0EZwHMud4TueVKxhHinaj3PgyZhSm8Nj";

    async fn client(&self, node_id: NodeId) -> anyhow::Result<RaftServiceClient> {
        let addr = self.0.cluster_addrs[Into::<u64>::into(node_id) as usize];
        let mut conn = TcpStream::connect(addr).await?;
        conn.write_all(Self::RAFT_MARKER).await?;
        let transport = tarpc::serde_transport::new(
            LengthDelimitedCodec::builder().new_framed(conn),
            Bincode::default(),
        );
        Ok(RaftServiceClient::new(Default::default(), transport).spawn())
    }
}

#[tarpc::service]
pub trait RaftService {
    async fn append_entries(request: AppendEntries<Command>) -> RaftResult<AppendEntriesResponse>;
    async fn request_vote(request: RequestVote) -> RaftResult<RequestVoteResponse>;
}

#[derive(Clone)]
pub struct RaftServer(Arc<SharedState>);

impl RaftServer {
    pub(crate) fn new(shared: Arc<SharedState>) -> Self {
        Self(shared)
    }
}

#[tarpc::server]
impl RaftService for RaftServer {
    async fn append_entries(
        self,
        _: Context,
        request: AppendEntries<Command>,
    ) -> RaftResult<AppendEntriesResponse> {
        self.0.raft.append_entries(request).await
    }

    async fn request_vote(
        self,
        _: Context,
        request: RequestVote,
    ) -> RaftResult<RequestVoteResponse> {
        self.0.raft.request_vote(request).await
    }
}
