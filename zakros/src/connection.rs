mod cluster;
mod generic;
mod server;

use crate::{store::StoreCommand, Shared};
use bstr::ByteSlice;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{net::TcpStream, sync::TryAcquireError};
use tokio_util::codec::Framed;
use zakros_raft::Error as RaftError;
use zakros_redis::{
    command::{Arity, RedisCommand, SystemCommand, TransactionCommand},
    error::{ConnectionError, Error as RedisError, ResponseError},
    resp::{RespCodec, Value},
};

pub async fn serve(shared: Arc<Shared>, mut conn: TcpStream) -> std::io::Result<()> {
    match shared.conn_limit.clone().try_acquire_owned() {
        Ok(_permit) => RedisConnection::new(shared, conn).serve().await,
        Err(TryAcquireError::Closed) => unreachable!(),
        Err(TryAcquireError::NoPermits) => {
            use tokio::io::AsyncWriteExt;
            conn.write_all(b"-ERR max number of clients reached").await
        }
    }
}

struct RedisConnection {
    shared: Arc<Shared>,
    framed: Framed<TcpStream, RespCodec>,
    txn: Transaction,
    is_readonly: bool,
}

impl RedisConnection {
    pub(crate) fn new(shared: Arc<Shared>, conn: TcpStream) -> Self {
        Self {
            shared,
            framed: Framed::new(conn, RespCodec::default()),
            txn: Transaction::Inactive,
            is_readonly: false,
        }
    }

    pub async fn serve(mut self) -> std::io::Result<()> {
        while let Some(decoded) = self.framed.next().await {
            let strings = match decoded {
                Ok(strings) => strings,
                Err(ConnectionError::Io(err)) => return Err(err),
                Err(ConnectionError::Protocol(err)) => {
                    return self
                        .framed
                        .send(Err(ResponseError::ProtocolError(err).into()))
                        .await;
                }
            };
            tracing::trace!("received {:?}", DebugQuery(&strings));
            let Some((command, args)) = strings.split_first() else {
                continue;
            };
            match self.handle_command(command, args).await {
                Ok(()) => (),
                Err(Error::Io(err)) => return Err(err),
                Err(Error::Redis(err)) => self.framed.send(Err(err)).await?,
                Err(Error::Raft(err)) => match err {
                    RaftError::NotLeader { leader_id: None } => {
                        self.framed
                            .send(Err(RedisError::ClusterDown("No leader".to_owned())))
                            .await?
                    }
                    RaftError::NotLeader {
                        leader_id: Some(leader_id),
                    } => {
                        let addr =
                            &self.shared.opts.cluster_addrs[Into::<u64>::into(leader_id) as usize];
                        self.framed
                            .send(Err(RedisError::Moved {
                                slot: 0,
                                addr: *addr,
                            }))
                            .await?
                    }
                    RaftError::Shutdown => panic!("Raft server is shut down"),
                },
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: &[u8], args: &[Bytes]) -> Result<(), Error> {
        let command = match RedisCommand::try_from(command) {
            Ok(command) => command,
            Err(err) => {
                if matches!(self.txn, Transaction::Queued(_)) {
                    self.txn = Transaction::Error;
                }
                return Err(err.into());
            }
        };
        match command.arity() {
            Arity::Fixed(n) if args.len() == n => (),
            Arity::AtLeast(n) if args.len() >= n => (),
            _ => {
                if matches!(self.txn, Transaction::Queued(_)) {
                    self.txn = Transaction::Error;
                }
                return Err(RedisError::from(ResponseError::WrongArity).into());
            }
        }

        match command {
            RedisCommand::Transaction(command) => match command {
                TransactionCommand::Multi => match self.txn {
                    Transaction::Inactive => {
                        self.txn = Transaction::Queued(Default::default());
                        self.framed.send(Ok(Value::ok())).await?;
                        Ok(())
                    }
                    Transaction::Queued(_) | Transaction::Error => Err(RedisError::from(
                        ResponseError::Other("MULTI calls can not be nested"),
                    )
                    .into()),
                },
                TransactionCommand::Exec => match &mut self.txn {
                    Transaction::Inactive => {
                        Err(RedisError::from(ResponseError::Other("EXEC without MULTI")).into())
                    }
                    Transaction::Queued(queue) => {
                        let commands = std::mem::take(queue);
                        self.txn = Transaction::Inactive;
                        self.exec(commands).await
                    }
                    Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Err(RedisError::ExecAbort.into())
                    }
                },
                TransactionCommand::Discard => match self.txn {
                    Transaction::Inactive => {
                        Err(RedisError::from(ResponseError::Other("DISCARD without MULTI")).into())
                    }
                    Transaction::Queued(_) | Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        self.framed.send(Ok(Value::ok())).await?;
                        Ok(())
                    }
                },
            },
            RedisCommand::System(_) if !matches!(self.txn, Transaction::Inactive) => {
                self.txn = Transaction::Error;
                Err(RedisError::from(ResponseError::Other(
                    "Command not allowed inside a transaction",
                ))
                .into())
            }
            _ if !matches!(self.txn, Transaction::Inactive) => {
                if let Transaction::Queued(queue) = &mut self.txn {
                    queue.push((command, args.to_vec()));
                }
                self.framed.send(Ok("QUEUED".into())).await?;
                Ok(())
            }
            _ => self.call_command(command, args).await,
        }
    }

    async fn call_command(&mut self, command: RedisCommand, args: &[Bytes]) -> Result<(), Error> {
        let result = match command {
            RedisCommand::Write(command) => {
                self.shared
                    .raft
                    .write(StoreCommand::SingleWrite((command, args.to_vec())))
                    .await?
            }
            RedisCommand::Read(command) => {
                if !self.is_readonly {
                    self.shared.raft.read().await?;
                }
                command.call(&self.shared.store, args)
            }
            RedisCommand::Stateless(command) => command.call(args),
            RedisCommand::System(command) => match command {
                SystemCommand::Cluster => self.shared.cluster(args).await?,
                SystemCommand::Info => self.shared.info(args),
                SystemCommand::ReadOnly => self.readonly(args),
                SystemCommand::ReadWrite => self.readwrite(args),
                SystemCommand::Select => generic::select(args),
                SystemCommand::Shutdown => generic::shutdown(args),
            },
            RedisCommand::Transaction(_) => unreachable!(),
        };
        self.framed.send(result).await?;
        Ok(())
    }

    async fn exec(&mut self, commands: Vec<(RedisCommand, Vec<Bytes>)>) -> Result<(), Error> {
        let result = self.shared.raft.write(StoreCommand::Exec(commands)).await?;
        self.framed.send(result).await?;
        Ok(())
    }
}

#[derive(Default)]
enum Transaction {
    #[default]
    Inactive,
    Queued(Vec<(RedisCommand, Vec<Bytes>)>),
    Error,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Redis(#[from] RedisError),

    #[error(transparent)]
    Raft(#[from] RaftError),
}

struct DebugQuery<'a>(&'a [Bytes]);

impl std::fmt::Debug for DebugQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return f.write_str("(empty)");
        }
        for s in self.0 {
            if s.len() > 30 {
                write!(f, "{:?}... ", &s[..30].as_bstr())?;
            } else {
                write!(f, "{:?} ", s.as_bstr())?;
            }
        }
        Ok(())
    }
}
