use crate::connection::ConnectionError;
use bstr::ByteSlice;
use bytes::{Buf, BufMut, BytesMut};
use std::{io::Write, str::FromStr};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone)]
pub enum RedisValue {
    Null,
    SimpleString(&'static str),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RedisValue>),
}

impl From<&'static str> for RedisValue {
    fn from(value: &'static str) -> Self {
        Self::SimpleString(value)
    }
}

impl From<i64> for RedisValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<Vec<u8>> for RedisValue {
    fn from(value: Vec<u8>) -> Self {
        Self::BulkString(value)
    }
}

impl From<Vec<Self>> for RedisValue {
    fn from(value: Vec<Self>) -> Self {
        Self::Array(value)
    }
}

impl RedisValue {
    pub const fn ok() -> Self {
        Self::SimpleString("OK")
    }
}

pub struct QueryDecoder;

impl Decoder for QueryDecoder {
    type Item = Vec<Vec<u8>>;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        fn parse_bytes<T: FromStr>(s: &[u8]) -> Result<T, ConnectionError> {
            s.to_str()
                .map_err(|_| ConnectionError::Protocol)?
                .parse()
                .map_err(|_| ConnectionError::Protocol)
        }

        let mut bytes = &**src;

        let end = match bytes.find_byte(b'\r') {
            Some(end) if end + 1 < bytes.len() => end, // if there is a room for trailing \n
            _ => return Ok(None),
        };
        let len: i64 = match &bytes[..end] {
            [] => 0,                                           // empty
            [b'*', len_bytes @ ..] => parse_bytes(len_bytes)?, // array
            _ => return Err(ConnectionError::Protocol),
        };
        bytes = &bytes[end + 2..]; // 2 for skipping \r\n
        if len <= 0 {
            src.advance(src.len() - bytes.len()); // `bytes` has unconsumed bytes
            return Ok(Some(Vec::new()));
        }

        let mut strings = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let end = match bytes.find_byte(b'\r') {
                Some(end) if end + 1 < bytes.len() => end,
                _ => return Ok(None),
            };
            // expect bulk string
            let [b'$', len_bytes @ ..] = &bytes[..end] else {
                return Err(ConnectionError::Protocol);
            };
            bytes = &bytes[end + 2..];

            let len: usize = parse_bytes(len_bytes)?;
            if len + 2 > bytes.len() {
                return Ok(None);
            }
            strings.push(bytes[..len].to_vec());
            bytes = &bytes[len + 2..];
        }

        src.advance(src.len() - bytes.len());
        Ok(Some(strings))
    }
}

pub struct ResponseEncoder;

impl Encoder<RedisValue> for ResponseEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: RedisValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode(&mut dst.writer(), &item)
    }
}

fn encode<W: Write>(writer: &mut W, value: &RedisValue) -> std::io::Result<()> {
    match value {
        RedisValue::Null => {
            writer.write_all(b"$-1\r\n")?;
        }
        RedisValue::SimpleString(s) => {
            writer.write_all(b"+")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        RedisValue::Error(s) => {
            writer.write_all(b"-")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        RedisValue::Integer(i) => {
            write!(writer, ":{}\r\n", i)?;
        }
        RedisValue::BulkString(s) => {
            write!(writer, "${}\r\n", s.len())?;
            writer.write_all(s)?;
            writer.write_all(b"\r\n")?;
        }
        RedisValue::Array(a) => {
            write!(writer, "*{}\r\n", a.len())?;
            for x in a {
                encode(writer, x)?;
            }
        }
    }
    Ok(())
}
