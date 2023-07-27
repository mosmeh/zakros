use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, sync::Arc};
use zakros_raft::StateMachine;
use zakros_redis::{
    command::{RedisCommand, WriteCommand},
    resp::RedisValue,
    Dictionary, RedisResult,
};

#[derive(Clone, Default)]
pub struct Store(Arc<RwLock<Dictionary>>);

impl AsRef<RwLock<Dictionary>> for Store {
    fn as_ref(&self) -> &RwLock<Dictionary> {
        &self.0
    }
}

#[async_trait]
impl StateMachine for Store {
    type Command = Command;

    async fn apply(&mut self, command: Command) -> RedisResult {
        match command {
            Command::SingleWrite { command, args } => command.call(self.as_ref(), &args),
            Command::Exec(queries) => {
                let mut responses = Vec::with_capacity(queries.len());
                let dict = RefCell::new(self.0.write());
                for (command, args) in queries {
                    let response = match command {
                        RedisCommand::Write(command) => command.call(&dict, &args),
                        RedisCommand::Read(command) => command.call(&dict, &args),
                        RedisCommand::Stateless(command) => command.call(&args),
                        RedisCommand::Connection(_) => {
                            unreachable!()
                        }
                    };
                    responses.push(response.unwrap());
                }
                Ok(RedisValue::Array(responses))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    SingleWrite {
        command: WriteCommand,
        args: Vec<Vec<u8>>,
    },
    Exec(Vec<(RedisCommand, Vec<Vec<u8>>)>),
}

impl zakros_raft::Command for Command {
    type Output = RedisResult;
}
