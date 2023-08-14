use crate::{
    command::{Arity, ParsedCommand, RedisCommand, TransactionCommand},
    error::{Error, ResponseError},
    resp::Value,
    RedisResult,
};
use async_trait::async_trait;

#[derive(Default)]
pub struct RedisSession<H> {
    handler: H,
    txn: Transaction,
}

impl<H: SessionHandler> RedisSession<H> {
    pub const fn new(handler: H) -> Self {
        Self {
            handler,
            txn: Transaction::Inactive,
        }
    }

    pub async fn call(
        &mut self,
        command: &[u8],
        args: &[Vec<u8>],
    ) -> Result<RedisResult, H::Error> {
        let command = match ParsedCommand::try_from(command) {
            Ok(command) => command,
            Err(err) => {
                if matches!(self.txn, Transaction::Queued(_)) {
                    self.txn = Transaction::Error;
                }
                return Ok(Err(err));
            }
        };
        match command.arity() {
            Arity::Fixed(n) if args.len() == n => (),
            Arity::AtLeast(n) if args.len() >= n => (),
            _ => {
                if matches!(self.txn, Transaction::Queued(_)) {
                    self.txn = Transaction::Error;
                }
                return Ok(Err(ResponseError::WrongArity.into()));
            }
        }

        match command {
            ParsedCommand::Normal(command) => {
                if matches!(self.txn, Transaction::Inactive) {
                    return self.handler.call(command, args).await;
                }
                if let RedisCommand::System(_) = command {
                    self.txn = Transaction::Error;
                    return Ok(Err(ResponseError::Other(
                        "Command not allowed inside a transaction",
                    )
                    .into()));
                }
                if let Transaction::Queued(queue) = &mut self.txn {
                    queue.push((command, args.to_vec()));
                }
                Ok(Ok("QUEUED".into()))
            }
            ParsedCommand::Transaction(command) => match command {
                TransactionCommand::Multi => match self.txn {
                    Transaction::Inactive => {
                        self.txn = Transaction::Queued(Default::default());
                        Ok(Ok(Value::ok()))
                    }
                    Transaction::Queued(_) | Transaction::Error => Ok(Err(ResponseError::Other(
                        "MULTI calls can not be nested",
                    )
                    .into())),
                },
                TransactionCommand::Exec => match &mut self.txn {
                    Transaction::Inactive => {
                        Ok(Err(ResponseError::Other("EXEC without MULTI").into()))
                    }
                    Transaction::Queued(queue) => {
                        let commands = std::mem::take(queue);
                        self.txn = Transaction::Inactive;
                        self.handler.exec(commands).await
                    }
                    Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Ok(Err(Error::ExecAbort))
                    }
                },
                TransactionCommand::Discard => match self.txn {
                    Transaction::Inactive => {
                        Ok(Err(ResponseError::Other("DISCARD without MULTI").into()))
                    }
                    Transaction::Queued(_) | Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Ok(Ok(Value::ok()))
                    }
                },
            },
        }
    }
}

#[derive(Default)]
enum Transaction {
    #[default]
    Inactive,
    Queued(Vec<(RedisCommand, Vec<Vec<u8>>)>),
    Error,
}

#[async_trait]
pub trait SessionHandler {
    type Error;

    async fn call(
        &mut self,
        command: RedisCommand,
        args: &[Vec<u8>],
    ) -> Result<RedisResult, Self::Error>;

    async fn exec(
        &mut self,
        commands: Vec<(RedisCommand, Vec<Vec<u8>>)>,
    ) -> Result<RedisResult, Self::Error>;
}
