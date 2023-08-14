use crate::{store::StoreCommand, RaftResult, SharedState};
use async_trait::async_trait;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tarpc::{context::Context, tokio_serde::formats::Bincode};
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};
use tokio_util::codec::LengthDelimitedCodec;
use zakros_raft::{
    transport::{
        AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse, Transport,
    },
    NodeId,
};

pub struct RpcHandler {
    cluster_addrs: Vec<SocketAddr>,
    timeout: Duration,
}

impl RpcHandler {
    pub(crate) fn new(cluster_addrs: Vec<SocketAddr>) -> Self {
        Self {
            cluster_addrs,
            timeout: Duration::from_secs(1),
        }
    }
}

#[async_trait]
impl Transport for RpcHandler {
    type Command = StoreCommand;
    type Error = anyhow::Error;

    async fn send_append_entries(
        &self,
        dest: NodeId,
        request: AppendEntries<Self::Command>,
    ) -> Result<AppendEntriesResponse, Self::Error> {
        timeout(self.timeout, async {
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
        timeout(self.timeout, async {
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
    pub const RPC_MARKER: &[u8] = b"\0EZwHMud4TueVKxhHinaj3PgyZhSm8Nj";

    async fn client(&self, node_id: NodeId) -> anyhow::Result<RpcServiceClient> {
        let addr = self.cluster_addrs[Into::<u64>::into(node_id) as usize];
        let mut conn = TcpStream::connect(addr).await?;
        conn.write_all(Self::RPC_MARKER).await?;
        let transport = tarpc::serde_transport::new(
            LengthDelimitedCodec::builder().new_framed(conn),
            Bincode::default(),
        );
        Ok(RpcServiceClient::new(Default::default(), transport).spawn())
    }
}

#[tarpc::service]
pub trait RpcService {
    async fn append_entries(
        request: AppendEntries<StoreCommand>,
    ) -> RaftResult<AppendEntriesResponse>;

    async fn request_vote(request: RequestVote) -> RaftResult<RequestVoteResponse>;
}

#[derive(Clone)]
pub struct RpcServer(Arc<SharedState>);

impl RpcServer {
    pub(crate) fn new(shared: Arc<SharedState>) -> Self {
        Self(shared)
    }
}

#[tarpc::server]
impl RpcService for RpcServer {
    async fn append_entries(
        self,
        _: Context,
        request: AppendEntries<StoreCommand>,
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
