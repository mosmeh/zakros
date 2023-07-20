use crate::{
    command::{RedisCommand, TransactionCommand},
    error::{Error, TransactionError},
    resp::{QueryDecoder, RedisValue, ResponseEncoder},
    store::{Command, Query},
    RedisResult, SharedState,
};
use futures::{SinkExt, StreamExt};
use std::{ops::Deref, sync::Arc};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Protocol error")]
    Protocol,
}

pub enum Transaction {
    Inactive,
    Queued(Vec<Query>),
    Error,
}

impl Transaction {
    fn is_active(&self) -> bool {
        matches!(self, Self::Queued(_) | Self::Error)
    }
}

pub struct RedisConnection {
    pub shared: Arc<SharedState>,
    pub txn: Transaction,
}

impl RedisConnection {
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self {
            shared,
            txn: Transaction::Inactive,
        }
    }

    pub async fn serve(mut self, conn: TcpStream) -> std::io::Result<()> {
        let (reader, writer) = conn.into_split();
        let mut reader = FramedRead::new(reader, QueryDecoder);
        let mut writer = FramedWrite::new(writer, ResponseEncoder);
        while let Some(decoded) = reader.next().await {
            match decoded {
                Ok(strings) => {
                    let Some((command, args)) = strings.split_first() else {
                        continue;
                    };
                    if command.eq_ignore_ascii_case(b"QUIT") {
                        return Ok(());
                    }
                    let value = self.handle_query(command, args).await.unwrap();
                    writer.send(value).await?;
                }
                Err(ConnectionError::Io(err)) => return Err(err),
                Err(ConnectionError::Protocol) => {
                    let value = RedisValue::Error("ERR Protocol error".to_owned());
                    return writer.send(value).await;
                }
            }
        }
        Ok(())
    }

    async fn handle_query(&mut self, command: &[u8], args: &[Vec<u8>]) -> RedisResult {
        let result = self.handle_command(command, args).await;
        if let Transaction::Queued(_) = &self.txn {
            match &result {
                Ok(_) | Err(Error::Transaction(_)) => (),
                Err(_) => self.txn = Transaction::Error,
            }
        }
        match result {
            Ok(value) => Ok(value),
            Err(Error::Raft(zakros_raft::Error::NotLeader { leader_id: None })) => {
                Ok(RedisValue::Error("CLUSTERDOWN No leader".to_owned()))
            }
            Err(Error::Raft(zakros_raft::Error::NotLeader {
                leader_id: Some(leader_id),
            })) => {
                let addr = &self.shared.args.cluster_addrs[Into::<u64>::into(leader_id) as usize];
                Ok(RedisValue::Error(format!(
                    "MOVED 0 {}:{}",
                    addr.ip(),
                    addr.port()
                )))
            }
            Err(err @ Error::Raft(zakros_raft::Error::Shutdown)) => Err(err),
            Err(err) => Ok(RedisValue::Error(err.to_string())),
        }
    }

    async fn handle_command(&mut self, command: &[u8], args: &[Vec<u8>]) -> RedisResult {
        let command = RedisCommand::parse(command)?;
        // TODO: check arity

        match command {
            RedisCommand::Transaction(txn_command) => match txn_command {
                TransactionCommand::Multi => match self.txn {
                    Transaction::Inactive => {
                        self.txn = Transaction::Queued(Default::default());
                        Ok(RedisValue::ok())
                    }
                    Transaction::Queued(_) | Transaction::Error => {
                        Err(TransactionError::NestedMulti.into())
                    }
                },
                TransactionCommand::Exec => match &mut self.txn {
                    Transaction::Inactive => {
                        Err(TransactionError::CommandWithoutMulti(txn_command).into())
                    }
                    Transaction::Queued(queue) => {
                        let command = Command::Exec(std::mem::take(queue));
                        self.txn = Transaction::Inactive;
                        self.shared.raft.write(command).await?
                    }
                    Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Err(TransactionError::ExecAborted.into())
                    }
                },
                TransactionCommand::Discard => match self.txn {
                    Transaction::Inactive => {
                        Err(TransactionError::CommandWithoutMulti(txn_command).into())
                    }
                    Transaction::Queued(_) | Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Ok(RedisValue::ok())
                    }
                },
            },
            RedisCommand::Connection(_) if self.txn.is_active() => {
                Err(TransactionError::CommandInsideMulti(command).into())
            }
            _ if self.txn.is_active() => {
                if let Transaction::Queued(queue) = &mut self.txn {
                    queue.push(Query {
                        command,
                        args: args.to_owned(),
                    });
                }
                Ok("QUEUED".into())
            }
            RedisCommand::Write(command) => {
                self.shared
                    .raft
                    .write(Command::SingleWrite {
                        command,
                        args: args.to_owned(),
                    })
                    .await?
            }
            RedisCommand::Read(command) => {
                self.shared.raft.read().await?;
                command.call(self.shared.store.deref(), args)
            }
            RedisCommand::Stateless(command) => command.call(args),
            RedisCommand::Connection(command) => command.call(self, args).await,
        }
    }
}
