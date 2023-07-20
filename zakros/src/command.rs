mod generic;
mod list;
mod server;
mod string;

use std::net::SocketAddr;

use crate::{
    connection::RedisConnection,
    error::Error,
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};
use serde::{Deserialize, Serialize};
use zakros_raft::async_trait::async_trait;

macro_rules! commands {
    ($($name:ident => $variant:ident,)*) => {
        #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
        pub enum RedisCommand {
            $($variant,)*
        }

        impl std::fmt::Display for RedisCommand {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", match self {
                    $(Self::$variant => stringify!($name),)*
                })
            }
        }

        impl RedisCommand {
            pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
                $(
                    const $name: &[u8] = stringify!($name).as_bytes();
                )*
                match bytes.to_ascii_uppercase().as_slice() {
                    $($name => Ok(Self::$variant),)*
                    _ => Err(Error::UnknownCommand),
                }
            }

            pub const fn kind(&self) -> CommandKind {
                match self {
                    $(Self::$variant => $variant::KIND),*
                }
            }

            pub fn call<'a, D: RwLockable<'a, Dictionary>>(&self, dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
                match self {
                    $(Self::$variant => <$variant as Command<_>>::call(dict, args)),*
                }
            }
        }

        $(pub enum $variant {})*
    }
}

#[derive(PartialEq, Eq)]
pub enum CommandKind {
    Write,
    Read,
    Stateless,
    Connection,
}

mod marker {
    pub enum Write {}
    pub enum Read {}
    pub enum Stateless {}
    pub enum Connection {}
}

trait Command<T> {
    const KIND: CommandKind;

    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult;
}

pub trait WriteCommand {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult;
}

impl<C: WriteCommand> Command<marker::Write> for C {
    const KIND: CommandKind = CommandKind::Write;

    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        Self::call(dict, args)
    }
}

pub trait ReadCommand {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult;
}

impl<C: ReadCommand> Command<marker::Read> for C {
    const KIND: CommandKind = CommandKind::Read;

    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        Self::call(dict, args)
    }
}

pub trait StatelessCommand {
    fn call(args: &[Vec<u8>]) -> RedisResult;
}

impl<C: StatelessCommand> Command<marker::Stateless> for C {
    const KIND: CommandKind = CommandKind::Stateless;

    fn call<'a, D: ReadLockable<'a, Dictionary>>(_: &D, args: &[Vec<u8>]) -> RedisResult {
        Self::call(args)
    }
}

#[async_trait]
pub trait ConnectionCommand {
    async fn call(conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult;
}

impl<C: ConnectionCommand> Command<marker::Connection> for C {
    const KIND: CommandKind = CommandKind::Connection;

    fn call<'a, D: RwLockable<'a, Dictionary>>(_: &'a D, _: &[Vec<u8>]) -> RedisResult {
        unreachable!()
    }
}

commands! {
    APPEND => Append,
    CLUSTER => Cluster,
    DBSIZE => DbSize,
    DEL => Del,
    DISCARD => Discard,
    ECHO => Echo,
    EXEC => Exec,
    EXISTS => Exists,
    FLUSHALL => FlushAll,
    FLUSHDB => FlushDb,
    GET => Get,
    GETRANGE => GetRange,
    KEYS => Keys,
    LINDEX => LIndex,
    LLEN => LLen,
    LPOP => LPop,
    LPUSH => LPush,
    LPUSHX => LPushX,
    LRANGE => LRange,
    LSET => LSet,
    LTRIM => LTrim,
    MGET => MGet,
    MSET => MSet,
    MSETNX => MSetNx,
    MULTI => Multi,
    PING => Ping,
    RENAME => Rename,
    RENAMENX => RenameNx,
    RPOP => RPop,
    RPUSH => RPush,
    RPUSHX => RPushX,
    SET => Set,
    SETRANGE => SetRange,
    STRLEN => StrLen,
    TIME => Time,
    TYPE => Type,
}

#[async_trait]
impl ConnectionCommand for Cluster {
    async fn call(conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult {
        let &[subcommand, _args @ ..] = &args else {
            return Err(Error::WrongArity);
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"MYID" => Ok(conn.shared.args.id.to_string().into_bytes().into()),
            b"SLOTS" => {
                const CLUSTER_SLOTS: i64 = 16384;
                match conn.shared.raft.status().await?.leader_id {
                    Some(leader_id) => {
                        let mut response = vec![0.into(), (CLUSTER_SLOTS - 1).into()];
                        let leader_index = Into::<u64>::into(leader_id) as usize;
                        let addrs = &conn.shared.args.cluster_addrs;
                        response.reserve(addrs.len());
                        let addr = addrs[leader_index];
                        fn addr_to_value(addr: SocketAddr) -> RedisValue {
                            vec![
                                RedisValue::BulkString(addr.ip().to_string().into_bytes()),
                                (addr.port() as i64).into(),
                            ]
                            .into()
                        }
                        response.push(addr_to_value(addr));
                        for (i, addr) in addrs.iter().enumerate() {
                            if i != leader_index {
                                response.push(addr_to_value(*addr));
                            }
                        }
                        Ok(RedisValue::Array(vec![RedisValue::Array(response)]))
                    }
                    None => Err(Error::Raft(zakros_raft::Error::NotLeader {
                        leader_id: None,
                    })),
                }
            }
            b"INFO" | b"NODES" => todo!(),
            _ => Err(Error::UnknownSubcommand),
        }
    }
}

#[async_trait]
impl ConnectionCommand for Discard {
    async fn call(_: &mut RedisConnection, _: &[Vec<u8>]) -> RedisResult {
        unreachable!()
    }
}

#[async_trait]
impl ConnectionCommand for Exec {
    async fn call(_: &mut RedisConnection, _: &[Vec<u8>]) -> RedisResult {
        unreachable!()
    }
}

#[async_trait]
impl ConnectionCommand for Multi {
    async fn call(_: &mut RedisConnection, _: &[Vec<u8>]) -> RedisResult {
        unreachable!()
    }
}
