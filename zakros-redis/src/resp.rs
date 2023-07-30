use crate::{error::ConnectionError, RedisResult};
use bstr::ByteSlice;
use bytes::{Buf, BufMut, BytesMut};
use std::{fmt::Debug, io::Write, str::FromStr};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    SimpleString(&'static str),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<RedisResult>),
}

impl From<&'static str> for Value {
    fn from(value: &'static str) -> Self {
        Self::SimpleString(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::BulkString(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<Vec<RedisResult>> for Value {
    fn from(value: Vec<RedisResult>) -> Self {
        Self::Array(value)
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::SimpleString(s) => write!(f, "simple({:?})", s),
            Self::BulkString(s) => write!(f, "bulk({:?})", s.as_bstr()),
            Self::Integer(i) => write!(f, "int({})", i),
            Self::Array(values) => {
                f.write_str("array(")?;
                let mut is_first = true;
                for x in values {
                    if !is_first {
                        f.write_str(", ")?;
                    }
                    write!(f, "{:?}", x)?;
                    is_first = false;
                }
                f.write_str(")")
            }
        }
    }
}

impl Value {
    pub const fn ok() -> Self {
        Self::SimpleString("OK")
    }
}

pub struct QueryDecoder;

impl Decoder for QueryDecoder {
    type Item = Vec<Vec<u8>>;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        fn parse_bytes<T: FromStr>(s: &[u8]) -> Result<T, ()> {
            s.to_str().map_err(|_| ())?.parse().map_err(|_| ())
        }

        let mut bytes = &**src;

        let end = match bytes.find_byte(b'\r') {
            Some(end) if end + 1 < bytes.len() => end, // if there is a room for trailing \n
            _ => return Ok(None),
        };
        let len: i32 = match &bytes[..end] {
            [] => 0, // empty
            [b'*', len_bytes @ ..] => parse_bytes(len_bytes)
                .map_err(|_| ConnectionError::Protocol("invalid multibulk length".to_owned()))?, // array
            _ => {
                return Err(ConnectionError::Protocol(
                    "inline protocol is not implemented".to_owned(),
                ))
            }
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
                return Err(ConnectionError::Protocol(format!(
                    "expected '$', got '{}'",
                    char::from(bytes[0])
                )));
            };
            bytes = &bytes[end + 2..];

            let len: usize = parse_bytes(len_bytes)
                .map_err(|_| ConnectionError::Protocol("invalid bulk length".to_owned()))?;
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

impl Encoder<RedisResult> for ResponseEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: RedisResult, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode(&mut dst.writer(), &item)
    }
}

fn encode<W: Write>(writer: &mut W, value: &RedisResult) -> std::io::Result<()> {
    match value {
        Ok(Value::Null) => writer.write_all(b"$-1\r\n"),
        Ok(Value::SimpleString(s)) => {
            writer.write_all(b"+")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")
        }
        Ok(Value::BulkString(s)) => {
            write!(writer, "${}\r\n", s.len())?;
            writer.write_all(s)?;
            writer.write_all(b"\r\n")
        }
        Ok(Value::Integer(i)) => {
            write!(writer, ":{}\r\n", i)
        }
        Ok(Value::Array(values)) => {
            write!(writer, "*{}\r\n", values.len())?;
            for x in values {
                encode(writer, x)?;
            }
            Ok(())
        }
        Err(err) => {
            write!(writer, "-{}\r\n", err)
        }
    }
}
