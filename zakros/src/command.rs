mod cluster;
mod debug;
mod generic;
mod pubsub;
mod server;

use crate::{connection::RedisConnection, store::RaftCommand};
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
        RedisCommand::Write(command) => match &conn.shared.raft {
            Some(raft) => {
                raft.write(RaftCommand::SingleWrite((command, args.to_vec())))
                    .await?
            }
            None => command.call(&conn.shared.store, args),
        },
        RedisCommand::Read(command) => {
            match &conn.shared.raft {
                Some(raft) if !conn.is_readonly => raft.read().await?,
                _ => (),
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
    let result = match &conn.shared.raft {
        Some(raft) => raft.write(RaftCommand::Exec(commands)).await?,
        None => conn.shared.store.exec(commands),
    };
    conn.framed.send(result).await?;
    Ok(())
}
