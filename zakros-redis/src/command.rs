mod bitops;
mod cluster;
mod generic;
mod hash;
mod list;
mod server;
mod set;
mod string;
mod transaction;

use crate::{
    error::{Error, ResponseError},
    lockable::{ReadLockable, RwLockable},
    Dictionary, RedisResult,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RedisCommand {
    Write(WriteCommand),
    Read(ReadCommand),
    Stateless(StatelessCommand),
    System(SystemCommand),
}

impl Display for RedisCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Write(command) => write!(f, "{}", command),
            Self::Read(command) => write!(f, "{}", command),
            Self::Stateless(command) => write!(f, "{}", command),
            Self::System(command) => write!(f, "{}", command),
        }
    }
}

pub(crate) enum Arity {
    Fixed(usize),
    AtLeast(usize),
}

pub(crate) enum ParsedCommand {
    Normal(RedisCommand),
    Transaction(TransactionCommand),
}

impl From<RedisCommand> for ParsedCommand {
    fn from(value: RedisCommand) -> Self {
        Self::Normal(value)
    }
}

impl TryFrom<&[u8]> for ParsedCommand {
    type Error = Error;

    fn try_from(name: &[u8]) -> Result<Self, Self::Error> {
        let bytes = &name.to_ascii_uppercase();
        if let Some(command) = WriteCommand::parse(bytes) {
            return Ok(RedisCommand::Write(command).into());
        }
        if let Some(command) = ReadCommand::parse(bytes) {
            return Ok(RedisCommand::Read(command).into());
        }
        if let Some(command) = StatelessCommand::parse(bytes) {
            return Ok(RedisCommand::Stateless(command).into());
        }
        if let Some(command) = SystemCommand::parse(bytes) {
            return Ok(RedisCommand::System(command).into());
        }
        if let Some(command) = TransactionCommand::parse(bytes) {
            return Ok(Self::Transaction(command));
        }
        Err(ResponseError::UnknownCommand(String::from_utf8_lossy(name).into_owned()).into())
    }
}

impl ParsedCommand {
    pub const fn arity(&self) -> Arity {
        match self {
            Self::Normal(RedisCommand::Write(command)) => command.arity(),
            Self::Normal(RedisCommand::Read(command)) => command.arity(),
            Self::Normal(RedisCommand::Stateless(command)) => command.arity(),
            Self::Normal(RedisCommand::System(command)) => command.arity(),
            Self::Transaction(command) => command.arity(),
        }
    }
}

macro_rules! commands {
    ($kind:ident, $($id:ident,)*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $kind {
            $($id,)*
        }

        impl Display for $kind {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", match self {
                    $(Self::$id => $id::NAME,)*
                })
            }
        }

        impl $kind {
            #[allow(non_upper_case_globals)]
            const fn parse(bytes: &[u8]) -> Option<Self> {
                $(const $id: &[u8] = $id::NAME.as_bytes();)*
                match bytes {
                    $($id => Some(Self::$id),)*
                    _ => None,
                }
            }

            const fn arity(&self) -> Arity {
                match self {
                    $(Self::$id => $id::ARITY,)*
                }
            }
        }

        $(enum $id {})*
    }
}

macro_rules! write_commands {
    ($($id:ident,)*) => {
        commands!(WriteCommand, $($id,)*);

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
    ($($id:ident,)*) => {
        commands!(ReadCommand, $($id,)*);

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
    ($($id:ident,)*) => {
        commands!(StatelessCommand, $($id,)*);

        impl StatelessCommand {
            pub fn call(&self, args: &[Vec<u8>]) -> RedisResult {
                match self {
                    $(Self::$id => $id::call(args),)*
                }
            }
        }
    }
}

macro_rules! system_commands {
    ($($id:ident,)*) => {
        commands!(SystemCommand, $($id,)*);
    }
}

macro_rules! transaction_commands {
    ($($id:ident,)*) => {
        commands!(TransactionCommand, $($id,)*);
    }
}

write_commands! {
    Append,
    BitOp,
    Del,
    FlushAll,
    FlushDb,
    GetDel,
    GetSet,
    HDel,
    HMSet,
    HSet,
    HSetNx,
    LPop,
    LPush,
    LPushX,
    LSet,
    LTrim,
    MSet,
    MSetNx,
    Rename,
    RenameNx,
    RPop,
    RPopLPush,
    RPush,
    RPushX,
    SAdd,
    SDiffStore,
    Set,
    SetBit,
    SetNx,
    SetRange,
    SInterStore,
    SMove,
    SRem,
    SUnionStore,
}

read_commands! {
    BitCount,
    DbSize,
    Exists,
    Get,
    GetBit,
    GetRange,
    HExists,
    HGet,
    HGetAll,
    HKeys,
    HLen,
    HMGet,
    HStrLen,
    HVals,
    Keys,
    LIndex,
    LLen,
    LRange,
    MGet,
    SCard,
    SDiff,
    SInter,
    SIsMember,
    SMembers,
    SMIsMember,
    StrLen,
    SubStr,
    SUnion,
    Type,
}

stateless_commands! {
    Echo,
    Ping,
    Time,
}

system_commands! {
    Cluster,
    Info,
    ReadOnly,
    ReadWrite,
    Select,
    Shutdown,
}

transaction_commands! {
    Discard,
    Exec,
    Multi,
}

trait CommandSpec {
    const NAME: &'static str;
    const ARITY: Arity;
}

trait WriteCommandHandler: CommandSpec {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult;
}

trait ReadCommandHandler: CommandSpec {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult;
}

trait StatelessCommandHandler: CommandSpec {
    fn call(args: &[Vec<u8>]) -> RedisResult;
}
