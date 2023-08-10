mod cluster;
mod debug;
mod generic;
mod pubsub;
mod server;

use crate::{connection::RedisConnection, store::StoreCommand};
use bytes::Bytes;
use futures::SinkExt;
use zakros_raft::RaftError;
use zakros_redis::{
    command::{RedisCommand, SystemCommand},
    pubsub::SubscriberRecvError,
    RedisError,
};

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Redis(#[from] RedisError),

    #[error(transparent)]
    Raft(#[from] RaftError),

    #[error(transparent)]
    SubscriberRecv(#[from] SubscriberRecvError),
}

pub async fn call(
    conn: &mut RedisConnection,
    command: RedisCommand,
    args: &[Bytes],
) -> Result<(), CommandError> {
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
        RedisCommand::System(command) => {
            use cluster::*;
            use debug::*;
            use generic::*;
            use pubsub::*;
            use server::*;
            match command {
                SystemCommand::Cluster => Ok(cluster(conn, args).await?),
                SystemCommand::Debug => debug(conn, args),
                SystemCommand::Info => info(conn, args),
                SystemCommand::PSubscribe => return psubscribe(conn, args).await,
                SystemCommand::Publish => Ok(publish(conn, args).await?),
                SystemCommand::PubSub => pubsub(conn, args),
                SystemCommand::PUnsubscribe => return punsubscribe(conn, args).await,
                SystemCommand::ReadOnly => readonly(conn, args),
                SystemCommand::ReadWrite => readwrite(conn, args),
                SystemCommand::Select => select(args),
                SystemCommand::Shutdown => shutdown(args),
                SystemCommand::Subscribe => return subscribe(conn, args).await,
                SystemCommand::Unsubscribe => return unsubscribe(conn, args).await,
            }
        }
        RedisCommand::Transaction(_) => unreachable!(),
    };
    conn.framed.send(result).await?;
    Ok(())
}

pub async fn exec(
    conn: &mut RedisConnection,
    commands: Vec<(RedisCommand, Vec<Bytes>)>,
) -> Result<(), CommandError> {
    let result = conn.shared.raft.write(StoreCommand::Exec(commands)).await?;
    conn.framed.send(result).await?;
    Ok(())
}
