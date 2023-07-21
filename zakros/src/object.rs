use crate::error::Error;
use bstr::ByteSlice;
use std::{
    collections::{HashSet, VecDeque},
    ops::Deref,
};

pub enum RedisObject {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
}

impl From<Vec<u8>> for RedisObject {
    fn from(value: Vec<u8>) -> Self {
        Self::String(value)
    }
}

pub trait ArgExt {
    fn to_i64(&self) -> Result<i64, Error>;
    fn to_i32(&self) -> Result<i32, Error>;
    fn to_u32(&self) -> Result<u32, Error>;
}

impl<T: Deref<Target = [u8]>> ArgExt for T {
    fn to_i64(&self) -> Result<i64, Error> {
        self.to_str()
            .map_err(|_| Error::NotInteger)?
            .parse()
            .map_err(|_| Error::NotInteger)
    }

    fn to_i32(&self) -> Result<i32, Error> {
        self.to_str()
            .map_err(|_| Error::NotInteger)?
            .parse()
            .map_err(|_| Error::NotInteger)
    }

    fn to_u32(&self) -> Result<u32, Error> {
        self.to_str()
            .map_err(|_| Error::ValueOutOfRange)?
            .parse()
            .map_err(|_| Error::ValueOutOfRange)
    }
}
