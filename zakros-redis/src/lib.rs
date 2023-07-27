pub mod command;
pub mod error;
pub mod lockable;
pub mod resp;
pub mod session;

mod string;

use bstr::ByteSlice;
use error::Error;
use resp::RedisValue;
use std::collections::{HashMap, HashSet, VecDeque};

pub type RedisResult = Result<RedisValue, Error>;

pub enum RedisObject {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
}

impl From<Vec<u8>> for RedisObject {
    fn from(value: Vec<u8>) -> Self {
        Self::String(value)
    }
}

pub type Dictionary = HashMap<Vec<u8>, RedisObject>;

pub trait BytesExt {
    fn to_i32(&self) -> Result<i32, Error>;
    fn to_i64(&self) -> Result<i64, Error>;
    fn to_u64(&self) -> Result<u64, Error>;
}

impl<T: AsRef<[u8]>> BytesExt for T {
    fn to_i32(&self) -> Result<i32, Error> {
        self.as_ref()
            .to_str()
            .map_err(|_| Error::ValueOutOfRange)?
            .parse()
            .map_err(|_| Error::ValueOutOfRange)
    }

    fn to_i64(&self) -> Result<i64, Error> {
        self.as_ref()
            .to_str()
            .map_err(|_| Error::NotInteger)?
            .parse()
            .map_err(|_| Error::NotInteger)
    }

    fn to_u64(&self) -> Result<u64, Error> {
        self.as_ref()
            .to_str()
            .map_err(|_| Error::ValueOutOfRange)?
            .parse()
            .map_err(|_| Error::ValueOutOfRange)
    }
}
