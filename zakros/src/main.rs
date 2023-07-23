mod command;
mod connection;
mod error;
mod object;
mod resp;
mod rpc;
mod store;
mod string;

use clap::Parser;
use connection::RedisConnection;
use error::Error;
use resp::RedisValue;
use rpc::{RaftServer, RaftService, RpcHandler};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use store::{Command, Store};
use tarpc::{
    server::{BaseChannel, Channel},
    tokio_serde::formats::Bincode,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::LengthDelimitedCodec;
use zakros_raft::{storage::PersistentStorage, Config, NodeId, Raft};

type RedisResult = Result<RedisValue, Error>;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    #[arg(short = 'n', long, default_value_t = 0)]
    id: u64,

    #[arg(short = 'a', long, default_value = "0.0.0.0:6379")]
    bind_addr: SocketAddr,

    #[arg(short = 'c', long, value_delimiter = ',')]
    cluster_addrs: Vec<SocketAddr>,

    #[arg(short = 'd', long, default_value = "data")]
    dir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().map_err(anyhow::Error::msg)?;

    let mut args = Args::parse();
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

async fn serve(args: Args) -> anyhow::Result<()> {
    let listener = TcpListener::bind(args.bind_addr).await?;
    tracing::info!("bound to {}", listener.local_addr()?);
    let id = NodeId::from(args.id);
    let shared = Arc::new(SharedState::new(id, args).await);
    loop {
        let (mut conn, _) = listener.accept().await?;
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
                    .execute(RaftServer(shared).serve())
                    .await;
            } else {
                let _ = RedisConnection::new(shared).serve(conn).await;
            }
        });
    }
}

pub struct SharedState {
    args: Args,
    raft: Raft<Command>,
    store: Store,
}

impl SharedState {
    async fn new(node_id: NodeId, args: Args) -> Self {
        let store = Store::default();
        let raft = Raft::spawn(
            node_id,
            (0..args.cluster_addrs.len() as u64)
                .map(NodeId::from)
                .collect(),
            Config::default(),
            store.clone(),
            PersistentStorage::new(args.dir.join(Into::<u64>::into(node_id).to_string()))
                .await
                .unwrap(),
            Arc::new(RpcHandler(args.clone())),
        );
        Self { raft, args, store }
    }
}
