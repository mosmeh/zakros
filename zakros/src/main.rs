mod connection;
mod rpc;
mod store;

use clap::Parser;
use connection::RedisConnection;
use rpc::{RaftServer, RaftService, RpcHandler};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::SystemTime};
use store::{Command, Store};
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

    let mut args = Opts::parse();
    if args.cluster_addrs.is_empty() {
        args.cluster_addrs.push(args.bind_addr);
    }

    tokio::runtime::Runtime::new()?.block_on(async { tokio::spawn(serve(args)).await })??;
    Ok(())
}

async fn is_raft(conn: &mut TcpStream) -> std::io::Result<bool> {
    let mut buf = [0; RpcHandler::RAFT_MARKER.len()];
    loop {
        match conn.peek(&mut buf).await {
            Ok(len) if buf[..len] != RpcHandler::RAFT_MARKER[..len] => return Ok(false),
            Ok(len) if len == RpcHandler::RAFT_MARKER.len() => {
                conn.read_exact(&mut buf).await?;
                return Ok(true);
            }
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            Ok(_) => (),
            Err(err) => return Err(err),
        }
    }
}

async fn serve(args: Opts) -> anyhow::Result<()> {
    let listener = TcpListener::bind(args.bind_addr).await?;
    tracing::info!("bound to {}", listener.local_addr()?);
    let id = NodeId::from(args.id);
    let shared = Arc::new(SharedState::new(id, args).await);
    loop {
        let (mut conn, addr) = listener.accept().await?;
        tracing::trace!("accepting connection: {}", addr);
        let shared = shared.clone();
        tokio::spawn(async move {
            let Ok(is_raft) = is_raft(&mut conn).await else {
                return;
            };
            if is_raft {
                let transport = tarpc::serde_transport::new(
                    LengthDelimitedCodec::builder().new_framed(conn),
                    Bincode::default(),
                );
                BaseChannel::with_defaults(transport)
                    .execute(RaftServer::new(shared).serve())
                    .await;
            } else {
                let _ = RedisConnection::new(shared).serve(conn).await;
            }
        });
    }
}

struct SharedState {
    opts: Opts,
    raft: Raft<Command>,
    store: Store,
    started_at: SystemTime,
    conn_limit: Arc<Semaphore>,
}

impl SharedState {
    async fn new(node_id: NodeId, opts: Opts) -> Self {
        let store = Store::default();
        let raft = Raft::spawn(
            node_id,
            (0..opts.cluster_addrs.len() as u64)
                .map(NodeId::from)
                .collect(),
            Config::default(),
            store.clone(),
            PersistentStorage::new(opts.dir.join(Into::<u64>::into(node_id).to_string()))
                .await
                .unwrap(),
            Arc::new(RpcHandler(opts.clone())),
        );
        let conn_limit = Arc::new(Semaphore::new(opts.max_num_clients));
        Self {
            raft,
            opts,
            store,
            started_at: SystemTime::now(),
            conn_limit,
        }
    }
}
