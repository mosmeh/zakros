use crate::{
    command::{Arity, ParsedCommand, RedisCommand, TransactionCommand},
    error::{Error, TransactionError},
    resp::RedisValue,
    RedisResult,
};
use async_trait::async_trait;

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
        let result = self.handle_command(command, args).await?;
        if let Transaction::Queued(_) = &self.txn {
            match &result {
                Ok(_) | Err(Error::Transaction(_)) => (),
                Err(_) => self.txn = Transaction::Error,
            }
        }
        Ok(result)
    }

    async fn handle_command(
        &mut self,
        command: &[u8],
        args: &[Vec<u8>],
    ) -> Result<RedisResult, H::Error> {
        let command = match ParsedCommand::try_from(command) {
            Ok(command) => command,
            Err(err) => return Ok(Err(err)),
        };
        match command.arity() {
            Arity::Fixed(n) if args.len() == n => (),
            Arity::AtLeast(n) if args.len() >= n => (),
            _ => return Ok(Err(Error::WrongArity)),
        }

        match command {
            ParsedCommand::Normal(command) => {
                if !self.txn.is_active() {
                    return self.handler.call(command, args).await;
                }
                if let RedisCommand::Connection(_) = command {
                    return Ok(Err(TransactionError::CommandInsideMulti(command).into()));
                }
                if let Transaction::Queued(queue) = &mut self.txn {
                    queue.push((command, args.to_owned()));
                }
                Ok(Ok("QUEUED".into()))
            }
            ParsedCommand::Transaction(command) => match command {
                TransactionCommand::Multi => match self.txn {
                    Transaction::Inactive => {
                        self.txn = Transaction::Queued(Default::default());
                        Ok(Ok(RedisValue::ok()))
                    }
                    Transaction::Queued(_) | Transaction::Error => {
                        Ok(Err(TransactionError::NestedMulti.into()))
                    }
                },
                TransactionCommand::Exec => match &mut self.txn {
                    Transaction::Inactive => {
                        Ok(Err(TransactionError::CommandWithoutMulti(command).into()))
                    }
                    Transaction::Queued(queue) => {
                        let commands = std::mem::take(queue);
                        self.txn = Transaction::Inactive;
                        self.handler.exec(commands).await
                    }
                    Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Ok(Err(TransactionError::ExecAborted.into()))
                    }
                },
                TransactionCommand::Discard => match self.txn {
                    Transaction::Inactive => {
                        Ok(Err(TransactionError::CommandWithoutMulti(command).into()))
                    }
                    Transaction::Queued(_) | Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Ok(Ok(RedisValue::ok()))
                    }
                },
            },
        }
    }
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

enum Transaction {
    Inactive,
    Queued(Vec<(RedisCommand, Vec<Vec<u8>>)>),
    Error,
}

impl Transaction {
    const fn is_active(&self) -> bool {
        matches!(self, Self::Queued(_) | Self::Error)
    }
}
