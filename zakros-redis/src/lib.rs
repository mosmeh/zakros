pub mod command;
pub mod error;
pub mod lockable;
pub mod resp;

mod string;

use bstr::ByteSlice;
use bytes::Bytes;
use error::{Error, ResponseError};
use resp::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use string::string_match;

pub type RedisResult = Result<Value, Error>;

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

pub type Dictionary = HashMap<Bytes, Object>;

pub trait BytesExt {
    fn to_i32(&self) -> Result<i32, Error>;
    fn to_i64(&self) -> Result<i64, Error>;
    fn to_u64(&self) -> Result<u64, Error>;
}

impl<T: AsRef<[u8]>> BytesExt for T {
    fn to_i32(&self) -> Result<i32, Error> {
        self.as_ref()
            .to_str()
            .map_err(|_| Error::Response(ResponseError::ValueOutOfRange))?
            .parse()
            .map_err(|_| Error::Response(ResponseError::ValueOutOfRange))
    }

    fn to_i64(&self) -> Result<i64, Error> {
        self.as_ref()
            .to_str()
            .map_err(|_| Error::Response(ResponseError::NotInteger))?
            .parse()
            .map_err(|_| Error::Response(ResponseError::NotInteger))
    }

    fn to_u64(&self) -> Result<u64, Error> {
        self.as_ref()
            .to_str()
            .map_err(|_| Error::Response(ResponseError::ValueOutOfRange))?
            .parse()
            .map_err(|_| Error::Response(ResponseError::ValueOutOfRange))
    }
}
