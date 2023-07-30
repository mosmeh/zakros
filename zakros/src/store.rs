use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, sync::Arc};
use zakros_raft::StateMachine;
use zakros_redis::{
    command::{RedisCommand, WriteCommand},
    resp::Value,
    Dictionary, RedisResult,
};

#[derive(Clone, Default)]
pub struct Store(Arc<RwLock<Dictionary>>);

impl Store {
    pub fn new() -> Self {
        Default::default()
    }
}

impl AsRef<RwLock<Dictionary>> for Store {
    fn as_ref(&self) -> &RwLock<Dictionary> {
        &self.0
    }
}

#[async_trait]
impl StateMachine for Store {
    type Command = StoreCommand;

    async fn apply(&mut self, command: StoreCommand) -> RedisResult {
        match command {
            StoreCommand::SingleWrite((command, args)) => command.call(self.as_ref(), &args),
            StoreCommand::Exec(queries) => {
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
                    responses.push(response);
                }
                Ok(Value::Array(responses))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreCommand {
    SingleWrite((WriteCommand, Vec<Vec<u8>>)),
    Exec(Vec<(RedisCommand, Vec<Vec<u8>>)>),
}

impl zakros_raft::Command for StoreCommand {
    type Output = RedisResult;
}
