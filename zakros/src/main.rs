mod connection;
mod rpc;
mod store;

use clap::Parser;
use connection::RedisConnection;
use rpc::{RpcHandler, RpcServer, RpcService};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::SystemTime};
use store::{Store, StoreCommand};
use tarpc::{
    server::{BaseChannel, Channel},
    tokio_serde::formats::Bincode,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_util::codec::LengthDelimitedCodec;
use zakros_raft::{storage::PersistentStorage, Config, NodeId, Raft};

type RaftResult<T> = Result<T, zakros_raft::Error>;

#[derive(Debug, Clone, Parser)]
struct Opts {
    #[arg(short = 'n', long, default_value_t = 0)]
    id: u64,

    #[arg(short = 'a', long, default_value = "0.0.0.0:6379")]
    bind_addr: SocketAddr,

    #[arg(short = 'c', long, value_delimiter = ',')]
    cluster_addrs: Vec<SocketAddr>,

    #[arg(short = 'd', long, default_value = "data")]
    dir: PathBuf,

    #[arg(long, default_value_t = 10000)]
    max_num_clients: usize,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().map_err(anyhow::Error::msg)?;

    let mut opts = Opts::parse();
    if opts.cluster_addrs.is_empty() {
        opts.cluster_addrs.push(opts.bind_addr);
    }

    tokio::runtime::Runtime::new()?.block_on(async { tokio::spawn(serve(opts)).await })??;
    Ok(())
}

async fn is_rpc(conn: &mut TcpStream) -> std::io::Result<bool> {
    let mut buf = [0; RpcHandler::RPC_MARKER.len()];
    loop {
        match conn.peek(&mut buf).await {
            Ok(len) if buf[..len] != RpcHandler::RPC_MARKER[..len] => return Ok(false),
            Ok(len) if len == RpcHandler::RPC_MARKER.len() => {
                conn.read_exact(&mut buf).await?;
                return Ok(true);
            }
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            Ok(_) => (),
            Err(err) => return Err(err),
        }
    }
}

async fn serve(opts: Opts) -> anyhow::Result<()> {
    let listener = TcpListener::bind(opts.bind_addr).await?;
    tracing::info!("bound to {}", listener.local_addr()?);
    let id = NodeId::from(opts.id);
    let shared = Arc::new(SharedState::new(id, opts).await?);
    loop {
        let (mut conn, addr) = listener.accept().await?;
        tracing::trace!("accepting connection: {}", addr);
        let shared = shared.clone();
        tokio::spawn(async move {
            let Ok(is_rpc) = is_rpc(&mut conn).await else {
                return;
            };
            if is_rpc {
                let transport = tarpc::serde_transport::new(
                    LengthDelimitedCodec::builder().new_framed(conn),
                    Bincode::default(),
                );
                BaseChannel::with_defaults(transport)
                    .execute(RpcServer::new(shared).serve())
                    .await;
            } else {
                let _ = RedisConnection::new(shared).serve(conn).await;
            }
        });
    }
}

struct SharedState {
    opts: Opts,
    raft: Raft<StoreCommand>,
    store: Store,
    started_at: SystemTime,
    conn_limit: Arc<Semaphore>,
}

impl SharedState {
    async fn new(node_id: NodeId, opts: Opts) -> anyhow::Result<Self> {
        let started_at = SystemTime::now();
        let nodes = (0..opts.cluster_addrs.len() as u64)
            .map(NodeId::from)
            .collect();
        let store = Store::new();
        let storage =
            PersistentStorage::new(opts.dir.join(Into::<u64>::into(node_id).to_string())).await?;
        let rpc_handler = Arc::new(RpcHandler::new(opts.cluster_addrs.clone()));
        let raft = Raft::new(
            node_id,
            nodes,
            Config::default(),
            store.clone(),
            storage,
            rpc_handler,
        );
        let conn_limit = Arc::new(Semaphore::new(opts.max_num_clients));
        Ok(Self {
            raft,
            opts,
            store,
            started_at,
            conn_limit,
        })
    }
}
