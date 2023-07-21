use crate::{
    command::{RedisCommand, WriteCommand},
    object::RedisObject,
    resp::RedisValue,
    RedisResult,
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Ref, RefCell, RefMut},
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use zakros_raft::{async_trait::async_trait, StateMachine};

pub type Dictionary = HashMap<Vec<u8>, RedisObject>;

#[derive(Clone, Default)]
pub struct Store(Arc<RwLock<Dictionary>>);

impl Deref for Store {
    type Target = RwLock<Dictionary>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub command: RedisCommand,
    pub args: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    SingleWrite {
        command: WriteCommand,
        args: Vec<Vec<u8>>,
    },
    Exec(Vec<Query>),
}

#[async_trait]
impl StateMachine for Store {
    type Command = Command;
    type Output = RedisResult;

    async fn apply(&mut self, command: Self::Command) -> Self::Output {
        match command {
            Command::SingleWrite { command, args } => command.call(self.deref().deref(), &args),
            Command::Exec(queries) => {
                let mut responses = Vec::with_capacity(queries.len());
                let dict = RefCell::new(self.write());
                for Query { command, args } in queries {
                    let response = match command {
                        RedisCommand::Write(command) => command.call(&dict, &args),
                        RedisCommand::Read(command) => command.call(&dict, &args),
                        RedisCommand::Stateless(command) => command.call(&args),
                        RedisCommand::Connection(_) | RedisCommand::Transaction(_) => {
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

pub trait RwLockable<'a, T> {
    type ReadGuard: Deref<Target = T>;
    type WriteGuard: DerefMut<Target = T>;

    fn read(&'a self) -> Self::ReadGuard;
    fn write(&'a self) -> Self::WriteGuard;
}

impl<'a, T: 'a> RwLockable<'a, T> for RwLock<T> {
    type ReadGuard = RwLockReadGuard<'a, T>;
    type WriteGuard = RwLockWriteGuard<'a, T>;

    fn read(&'a self) -> Self::ReadGuard {
        self.read()
    }

    fn write(&'a self) -> Self::WriteGuard {
        self.write()
    }
}

impl<'a, T: 'a> RwLockable<'a, T> for RefCell<RwLockWriteGuard<'_, T>> {
    type ReadGuard = Ref<'a, T>;
    type WriteGuard = RefMut<'a, T>;

    fn read(&'a self) -> Self::ReadGuard {
        Ref::map(self.borrow(), |s| s.deref())
    }

    fn write(&'a self) -> Self::WriteGuard {
        RefMut::map(self.borrow_mut(), |s| s.deref_mut())
    }
}

pub trait ReadLockable<'a, T> {
    type Guard: Deref<Target = T>;

    fn read(&'a self) -> Self::Guard;
}

impl<'a, T, L> ReadLockable<'a, T> for L
where
    T: 'a,
    L: RwLockable<'a, T>,
{
    type Guard = L::ReadGuard;

    fn read(&'a self) -> Self::Guard {
        self.read()
    }
}
