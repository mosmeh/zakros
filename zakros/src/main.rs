mod command;
mod config;
mod connection;
mod rpc;
mod store;

use config::{Config, RaftStorageKind};
use rand::seq::SliceRandom;
use rpc::{RpcClient, RpcServer, RpcService};
use std::{sync::Arc, time::SystemTime};
use store::{RaftCommand, Store};
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
    config::RaftConfig,
    storage::{DiskStorage, MemoryStorage},
    NodeId, Raft,
};
use zakros_redis::pubsub::Publisher;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().map_err(anyhow::Error::msg)?;

    let config = Config::from_args()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.worker_threads.get())
        .build()?
        .block_on(async { tokio::spawn(serve(config)).await })?
        .map_err(Into::into)
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

async fn serve(config: Config) -> anyhow::Result<()> {
    let listener = TcpListener::bind((config.bind, config.port)).await?;
    tracing::info!("bound to {}", listener.local_addr()?);
    let shared = Arc::new(Shared::new(config).await?);
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

const RUN_ID_LEN: usize = 40;

pub struct Shared {
    config: Config,
    store: Store,
    raft: Option<Raft<RaftCommand>>,
    rpc_client: Arc<RpcClient>,
    publisher: Publisher,
    run_id: [u8; RUN_ID_LEN],
    started_at: SystemTime,
    conn_limit: Arc<Semaphore>,
}

impl Shared {
    async fn new(config: Config) -> anyhow::Result<Self> {
        let started_at = SystemTime::now();

        let mut run_id = [0; RUN_ID_LEN];
        {
            const CHARSET: &[u8] = b"0123456789abcdef";
            let mut rng = rand::thread_rng();
            for x in run_id.iter_mut() {
                *x = *CHARSET.choose(&mut rng).unwrap();
            }
        }

        let store = Store::new();
        let rpc_client = Arc::new(RpcClient::new(config.cluster_addrs.clone()));

        let raft = if config.raft_enabled {
            let node_id = NodeId::from(config.raft_node_id);
            let nodes = (0..config.cluster_addrs.len() as u64)
                .map(NodeId::from)
                .collect();
            let raft_config = RaftConfig::default();
            let store = store.clone();
            let rpc_client = rpc_client.clone();
            let raft = match config.raft_storage {
                RaftStorageKind::Disk => {
                    let storage =
                        DiskStorage::new(config.dir.join(Into::<u64>::into(node_id).to_string()))
                            .await?;
                    Raft::new(node_id, nodes, raft_config, store, storage, rpc_client)
                }
                RaftStorageKind::Memory => {
                    let storage = MemoryStorage::new();
                    Raft::new(node_id, nodes, raft_config, store, storage, rpc_client)
                }
            };
            Some(raft)
        } else {
            None
        };

        let publisher = Publisher::new(32768);
        let conn_limit = Arc::new(Semaphore::new(config.max_clients));

        Ok(Self {
            config,
            store,
            raft,
            rpc_client,
            publisher,
            run_id,
            started_at,
            conn_limit,
        })
    }
}
