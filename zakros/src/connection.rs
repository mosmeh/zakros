use crate::{
    command::{self, CommandError},
    Shared,
};
use bstr::ByteSlice;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{net::TcpStream, sync::TryAcquireError};
use tokio_util::codec::Framed;
use zakros_raft::RaftError;
use zakros_redis::{
    command::{Arity, RedisCommand, TransactionCommand},
    pubsub::{Subscriber, SubscriberRecvError},
    resp::{RespCodec, RespError, Value},
    RedisError, ResponseError,
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

pub struct RedisConnection {
    pub shared: Arc<Shared>,
    pub framed: Framed<TcpStream, RespCodec>,
    pub is_readonly: bool,
    pub subscriber: Subscriber,
    txn: Transaction,
}

impl RedisConnection {
    fn new(shared: Arc<Shared>, conn: TcpStream) -> Self {
        let subscriber = shared.publisher.subscriber();
        Self {
            shared,
            framed: Framed::new(conn, RespCodec::default()),
            is_readonly: false,
            subscriber,
            txn: Transaction::Inactive,
        }
    }

    async fn serve(mut self) -> std::io::Result<()> {
        while let Some(decoded) = self.framed.next().await {
            // TODO: reuse the `decoded` buffer to avoid the allocation
            //       on every command
            let strings = match decoded {
                Ok(strings) => strings,
                Err(RespError::Io(err)) => return Err(err),
                Err(RespError::Protocol(err)) => {
                    return self
                        .framed
                        .send(Err(ResponseError::ProtocolError(err).into()))
                        .await;
                }
            };
            tracing::trace!("received {:?}", QueryDebug(&strings));
            let Some((command, args)) = strings.split_first() else {
                continue;
            };
            match self.handle_command(command, args).await {
                Ok(()) => (),
                Err(CommandError::Io(err)) => return Err(err),
                Err(CommandError::Redis(err)) => self.framed.send(Err(err)).await?,
                Err(CommandError::Raft(err)) => match err {
                    RaftError::NotLeader { leader_id: None } => {
                        self.framed
                            .send(Err(RedisError::ClusterDown("No leader".to_owned())))
                            .await?
                    }
                    RaftError::NotLeader {
                        leader_id: Some(leader_id),
                    } => {
                        let addr = &self.shared.config.cluster_addrs
                            [Into::<u64>::into(leader_id) as usize];
                        self.framed
                            .send(Err(RedisError::Moved {
                                slot: 0,
                                addr: *addr,
                            }))
                            .await?
                    }
                    RaftError::Shutdown => {
                        // TODO: gracefully shut down server
                        panic!("Raft server is shut down")
                    }
                },
                Err(CommandError::SubscriberRecv(SubscriberRecvError::Lagged)) => return Ok(()),
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: &[u8], args: &[Bytes]) -> Result<(), CommandError> {
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
                        command::exec(self, commands).await
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
            _ => command::call(self, command, args).await,
        }
    }
}

#[derive(Default)]
enum Transaction {
    #[default]
    Inactive,
    Queued(Vec<(RedisCommand, Vec<Bytes>)>),
    Error,
}

struct QueryDebug<'a>(&'a [Bytes]);

impl std::fmt::Debug for QueryDebug<'_> {
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
