use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, sync::Arc};
use zakros_raft::StateMachine;
use zakros_redis::{
    command::{RedisCommand, WriteCommand},
    lockable::RwLockable,
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

#[async_trait]
impl StateMachine for Store {
    type Command = StoreCommand;

    async fn apply(&mut self, command: StoreCommand) -> RedisResult {
        match command {
            StoreCommand::SingleWrite((command, args)) => command.call(self, &args),
            StoreCommand::Exec(queries) => {
                let mut responses = Vec::with_capacity(queries.len());
                let dict = RefCell::new(self.0.write());
                for (command, args) in queries {
                    let response = match command {
                        RedisCommand::Write(command) => command.call(&dict, &args),
                        RedisCommand::Read(command) => command.call(&dict, &args),
                        RedisCommand::Stateless(command) => command.call(&args),
                        RedisCommand::System(_) | RedisCommand::Transaction(_) => {
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

impl<'a> RwLockable<'a, Dictionary> for Store {
    type ReadGuard = RwLockReadGuard<'a, Dictionary>;
    type WriteGuard = RwLockWriteGuard<'a, Dictionary>;

    fn read(&'a self) -> Self::ReadGuard {
        self.0.read()
    }

    fn write(&'a self) -> Self::WriteGuard {
        self.0.write()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreCommand {
    SingleWrite((WriteCommand, Vec<Bytes>)),
    Exec(Vec<(RedisCommand, Vec<Bytes>)>),
}

impl zakros_raft::Command for StoreCommand {
    type Output = RedisResult;
}
