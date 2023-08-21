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

    pub fn exec(&self, commands: Vec<(RedisCommand, Vec<Bytes>)>) -> RedisResult {
        let mut responses = Vec::with_capacity(commands.len());
        let dict = RefCell::new(self.0.write());
        for (command, args) in commands {
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

#[async_trait]
impl StateMachine for Store {
    type Command = RaftCommand;

    async fn apply(&mut self, command: RaftCommand) -> RedisResult {
        match command {
            RaftCommand::SingleWrite((command, args)) => command.call(self, &args),
            RaftCommand::Exec(commands) => self.exec(commands),
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
pub enum RaftCommand {
    SingleWrite((WriteCommand, Vec<Bytes>)),
    Exec(Vec<(RedisCommand, Vec<Bytes>)>),
}

impl zakros_raft::Command for RaftCommand {
    type Output = RedisResult;
}
