pub mod command;
pub mod lockable;
pub mod pubsub;
pub mod resp;
pub mod string;

use bstr::ByteSlice;
use bytes::Bytes;
use resp::{ProtocolError, Value};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
};

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum RedisError {
    #[error("ERR {0}")]
    Response(#[from] ResponseError),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("EXECABORT Transaction discarded because of previous errors.")]
    ExecAbort,

    #[error("MOVED {slot} {addr}")]
    Moved { slot: u16, addr: SocketAddr },

    #[error("CLUSTERDOWN {0}")]
    ClusterDown(String),
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ResponseError {
    #[error("Protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),

    #[error("unknown command '{0:.128}'")]
    UnknownCommand(String),

    #[error("unknown subcommand '{0:.128}'")]
    UnknownSubcommand(String),

    #[error("syntax error")]
    SyntaxError,

    #[error("wrong number of arguments")]
    WrongArity,

    #[error("value is not an integer or out of range")]
    NotInteger,

    #[error("value is out of range")]
    ValueOutOfRange,

    #[error("index out of range")]
    IndexOutOfRange,

    #[error("no such key")]
    NoKey,

    #[error("{0}")]
    Other(&'static str),
}

pub type RedisResult = Result<Value, RedisError>;

pub enum Object {
    String(Vec<u8>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
}

impl From<Vec<u8>> for Object {
    fn from(value: Vec<u8>) -> Self {
        Self::String(value)
    }
}

impl From<HashSet<Bytes>> for Object {
    fn from(value: HashSet<Bytes>) -> Self {
        Self::Set(value)
    }
}

impl From<HashMap<Bytes, Bytes>> for Object {
    fn from(value: HashMap<Bytes, Bytes>) -> Self {
        Self::Hash(value)
    }
}

pub type Dictionary = HashMap<Bytes, Object>;

pub trait BytesExt {
    fn to_i32(&self) -> Result<i32, RedisError>;
    fn to_i64(&self) -> Result<i64, RedisError>;
    fn to_u64(&self) -> Result<u64, RedisError>;
}

impl<T: AsRef<[u8]>> BytesExt for T {
    fn to_i32(&self) -> Result<i32, RedisError> {
        self.as_ref()
            .to_str()
            .map_err(|_| RedisError::Response(ResponseError::ValueOutOfRange))?
            .parse()
            .map_err(|_| RedisError::Response(ResponseError::ValueOutOfRange))
    }

    fn to_i64(&self) -> Result<i64, RedisError> {
        self.as_ref()
            .to_str()
            .map_err(|_| RedisError::Response(ResponseError::NotInteger))?
            .parse()
            .map_err(|_| RedisError::Response(ResponseError::NotInteger))
    }

    fn to_u64(&self) -> Result<u64, RedisError> {
        self.as_ref()
            .to_str()
            .map_err(|_| RedisError::Response(ResponseError::ValueOutOfRange))?
            .parse()
            .map_err(|_| RedisError::Response(ResponseError::ValueOutOfRange))
    }
}
