mod cluster;
mod generic;
mod pubsub;
mod server;

use crate::{connection::RedisConnection, store::StoreCommand};
use bytes::Bytes;
use cluster::*;
use futures::SinkExt;
use generic::*;
use pubsub::*;
use server::*;
use zakros_redis::{
    command::{RedisCommand, SystemCommand},
    pubsub::SubscriberRecvError,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Redis(#[from] zakros_redis::error::Error),

    #[error(transparent)]
    Raft(#[from] zakros_raft::Error),

    #[error(transparent)]
    SubscriberRecv(#[from] SubscriberRecvError),
}

pub async fn call(
    conn: &mut RedisConnection,
    command: RedisCommand,
    args: &[Bytes],
) -> Result<(), Error> {
    let result = match command {
        RedisCommand::Write(command) => {
            conn.shared
                .raft
                .write(StoreCommand::SingleWrite((command, args.to_vec())))
                .await?
        }
        RedisCommand::Read(command) => {
            if !conn.is_readonly {
                conn.shared.raft.read().await?;
            }
            command.call(&conn.shared.store, args)
        }
        RedisCommand::Stateless(command) => command.call(args),
        RedisCommand::System(command) => match command {
            SystemCommand::Cluster => cluster(conn, args).await?,
            SystemCommand::Info => info(conn, args),
            SystemCommand::PSubscribe => return psubscribe(conn, args).await,
            SystemCommand::Publish => return publish(conn, args).await,
            SystemCommand::PubSub => pubsub(conn, args),
            SystemCommand::PUnsubscribe => return punsubscribe(conn, args).await,
            SystemCommand::ReadOnly => readonly(conn, args),
            SystemCommand::ReadWrite => readwrite(conn, args),
            SystemCommand::Select => select(args),
            SystemCommand::Shutdown => shutdown(args),
            SystemCommand::Subscribe => return subscribe(conn, args).await,
            SystemCommand::Unsubscribe => return unsubscribe(conn, args).await,
        },
        RedisCommand::Transaction(_) => unreachable!(),
    };
    conn.framed.send(result).await?;
    Ok(())
}

pub async fn exec(
    conn: &mut RedisConnection,
    commands: Vec<(RedisCommand, Vec<Bytes>)>,
) -> Result<(), Error> {
    let result = conn.shared.raft.write(StoreCommand::Exec(commands)).await?;
    conn.framed.send(result).await?;
    Ok(())
}
