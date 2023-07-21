mod cluster;
mod generic;
mod list;
mod server;
mod set;
mod string;

use crate::{
    connection::RedisConnection,
    error::Error,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use zakros_raft::async_trait::async_trait;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisCommand {
    Write(WriteCommand),
    Read(ReadCommand),
    Stateless(StatelessCommand),
    Connection(ConnectionCommand),
    Transaction(TransactionCommand),
}

impl Display for RedisCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Write(command) => write!(f, "{}", command),
            Self::Read(command) => write!(f, "{}", command),
            Self::Stateless(command) => write!(f, "{}", command),
            Self::Connection(command) => write!(f, "{}", command),
            Self::Transaction(command) => write!(f, "{}", command),
        }
    }
}

impl RedisCommand {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let bytes = &bytes.to_ascii_uppercase();
        if let Some(command) = WriteCommand::parse(bytes) {
            return Ok(Self::Write(command));
        }
        if let Some(command) = ReadCommand::parse(bytes) {
            return Ok(Self::Read(command));
        }
        if let Some(command) = StatelessCommand::parse(bytes) {
            return Ok(Self::Stateless(command));
        }
        if let Some(command) = ConnectionCommand::parse(bytes) {
            return Ok(Self::Connection(command));
        }
        if let Some(command) = TransactionCommand::parse(bytes) {
            return Ok(Self::Transaction(command));
        }
        Err(Error::UnknownCommand)
    }
}

macro_rules! commands {
    (
        $kind:ident,
        $($name:ident => $id:ident,)*
    ) => {
        #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
        pub enum $kind {
            $($id,)*
        }

        impl Display for $kind {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", match self {
                    $(Self::$id => stringify!($name),)*
                })
            }
        }

        impl $kind {
            const fn parse(bytes: &[u8]) -> Option<Self> {
                $(
                    const $name: &[u8] = stringify!($name).as_bytes();
                )*
                match bytes {
                    $($name => Some(Self::$id),)*
                    _ => None,
                }
            }
        }
    }
}

macro_rules! write_commands {
    ($($name:ident => $id:ident,)*) => {
        commands!(WriteCommand, $($name => $id,)*);
        $(enum $id {})*

        impl WriteCommand {
            pub fn call<'a, D: RwLockable<'a, Dictionary>>(
                &self,
                dict: &'a D,
                args: &[Vec<u8>],
            ) -> RedisResult {
                match self {
                    $(Self::$id => $id::call(dict, args),)*
                }
            }
        }
    }
}

macro_rules! read_commands {
    ($($name:ident => $id:ident,)*) => {
        commands!(ReadCommand, $($name => $id,)*);
        $(enum $id {})*

        impl ReadCommand {
            pub fn call<'a, D: ReadLockable<'a, Dictionary>>(
                &self,
                dict: &'a D,
                args: &[Vec<u8>],
            ) -> RedisResult {
                match self {
                    $(Self::$id => $id::call(dict, args),)*
                }
            }
        }
    }
}

macro_rules! stateless_commands {
    ($($name:ident => $id:ident,)*) => {
        commands!(StatelessCommand, $($name => $id,)*);
        $(enum $id {})*

        impl StatelessCommand {
            pub fn call(&self, args: &[Vec<u8>]) -> RedisResult {
                match self {
                    $(Self::$id => $id::call(args),)*
                }
            }
        }
    }
}

macro_rules! connecion_commands {
    ($($name:ident => $id:ident,)*) => {
        commands!(ConnectionCommand, $($name => $id,)*);
        $(enum $id {})*

        impl ConnectionCommand {
            pub async fn call(&self, conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult {
                match self {
                    $(Self::$id => $id::call(conn, args).await,)*
                }
            }
        }
    }
}

macro_rules! transaction_commands {
    ($($name:ident => $id:ident,)*) => {
        commands!(TransactionCommand, $($name => $id,)*);
    }
}

write_commands! {
    APPEND => Append,
    DEL => Del,
    FLUSHALL => FlushAll,
    FLUSHDB => FlushDb,
    LPOP => LPop,
    LPUSH => LPush,
    LPUSHX => LPushX,
    LSET => LSet,
    LTRIM => LTrim,
    MSET => MSet,
    MSETNX => MSetNx,
    RENAME => Rename,
    RENAMENX => RenameNx,
    RPOP => RPop,
    RPUSH => RPush,
    RPUSHX => RPushX,
    SADD => SAdd,
    SDIFFSTORE => SDiffStore,
    SET => Set,
    SETRANGE => SetRange,
    SINTERSTORE => SInterStore,
    SMOVE => SMove,
    SREM => SRem,
    SUNIONSTORE => SUnionStore,
}

read_commands! {
    DBSIZE => DbSize,
    EXISTS => Exists,
    GET => Get,
    GETRANGE => GetRange,
    KEYS => Keys,
    LINDEX => LIndex,
    LLEN => LLen,
    LRANGE => LRange,
    MGET => MGet,
    SCARD => SCard,
    SDIFF => SDiff,
    SINTER => SInter,
    SISMEMBER => SIsMember,
    SMEMBERS => SMembers,
    STRLEN => StrLen,
    SUNION => SUnion,
    TYPE => Type,
}

stateless_commands! {
    ECHO => Echo,
    PING => Ping,
    SHUTDOWN => Shutdown,
    TIME => Time,
}

connecion_commands! {
    CLUSTER => Cluster,
    READONLY => ReadOnly,
    READWRITE => ReadWrite,
}

transaction_commands! {
    DISCARD => Discard,
    EXEC => Exec,
    MULTI => Multi,
}

trait WriteCommandHandler {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult;
}

trait ReadCommandHandler {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult;
}

trait StatelessCommandHandler {
    fn call(args: &[Vec<u8>]) -> RedisResult;
}

#[async_trait]
trait ConnectionCommandHandler {
    async fn call(conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult;
}
