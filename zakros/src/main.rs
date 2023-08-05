mod command;
mod connection;
mod rpc;
mod store;

use clap::{Parser, ValueEnum};
use rpc::{RpcClient, RpcServer, RpcService};
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
use zakros_raft::{
    config::Config,
    storage::{DiskStorage, MemoryStorage},
    NodeId, Raft,
};
use zakros_redis::pubsub::Publisher;

type RaftResult<T> = Result<T, zakros_raft::Error>;

#[derive(Debug, Clone, ValueEnum)]
enum RaftStorageKind {
    Disk,
    Memory,
}

#[derive(Debug, Clone, Parser)]
struct Opts {
    #[arg(short = 'n', long, default_value_t = 0)]
    id: u64,

    #[arg(short = 'a', long, default_value = "0.0.0.0:6379")]
    bind_addr: SocketAddr,

    #[arg(short = 'c', long, value_delimiter = ',')]
    cluster_addrs: Vec<SocketAddr>,

    #[arg(long, default_value_t = 10000)]
    max_num_clients: usize,

    #[arg(long, default_value = "disk")]
    storage: RaftStorageKind,

    #[arg(long, long, default_value = "data")]
    dir: PathBuf,
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
    let mut buf = [0; RpcClient::RPC_MARKER.len()];
    loop {
        match conn.peek(&mut buf).await {
            Ok(len) if buf[..len] != RpcClient::RPC_MARKER[..len] => return Ok(false),
            Ok(len) if len == RpcClient::RPC_MARKER.len() => {
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
    let shared = Arc::new(Shared::new(id, opts).await?);
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
                let _ = connection::serve(shared, conn).await;
            }
        });
    }
}

pub struct Shared {
    opts: Opts,
    raft: Raft<StoreCommand>,
    store: Store,
    rpc_client: Arc<RpcClient>,
    publisher: Publisher,
    started_at: SystemTime,
    conn_limit: Arc<Semaphore>,
}

impl Shared {
    async fn new(node_id: NodeId, opts: Opts) -> anyhow::Result<Self> {
        let started_at = SystemTime::now();
        let nodes = (0..opts.cluster_addrs.len() as u64)
            .map(NodeId::from)
            .collect();
        let store = Store::new();
        let rpc_client = Arc::new(RpcClient::new(opts.cluster_addrs.clone()));
        let raft = {
            let config = Config::default();
            let store = store.clone();
            let rpc_handler = rpc_client.clone();
            match opts.storage {
                RaftStorageKind::Disk => {
                    let storage =
                        DiskStorage::new(opts.dir.join(Into::<u64>::into(node_id).to_string()))
                            .await?;
                    Raft::new(node_id, nodes, config, store, storage, rpc_handler)
                }
                RaftStorageKind::Memory => {
                    let storage = MemoryStorage::new();
                    Raft::new(node_id, nodes, config, store, storage, rpc_handler)
                }
            }
        };
        let publisher = Publisher::new(32768);
        let conn_limit = Arc::new(Semaphore::new(opts.max_num_clients));
        Ok(Self {
            opts,
            raft,
            store,
            rpc_client,
            publisher,
            started_at,
            conn_limit,
        })
    }
}
