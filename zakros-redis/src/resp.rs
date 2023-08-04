use crate::{error::ConnectionError, RedisResult};
use bstr::ByteSlice;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{fmt::Debug, io::Write, str::FromStr};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    SimpleString(&'static str),
    BulkString(Bytes),
    Integer(i64),
    Array(Vec<RedisResult>),
}

impl From<&'static str> for Value {
    fn from(value: &'static str) -> Self {
        Self::SimpleString(value)
    }
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
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

#[derive(Default)]
pub struct RespCodec {
    array_len: Option<usize>,
    array: Vec<Bytes>,
}

impl Decoder for RespCodec {
    type Item = Vec<Bytes>;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        fn parse_bytes<T: FromStr>(s: &[u8]) -> Result<T, ()> {
            s.to_str().map_err(|_| ())?.parse().map_err(|_| ())
        }

        let array_len = match self.array_len {
            None => {
                let end = match src.find_byte(b'\r') {
                    Some(end) if end + 1 < src.len() => end, // if there is a room for trailing \n
                    _ => return Ok(None),
                };
                let len: i32 = match &src[..end] {
                    [] => 0, // empty
                    [b'*', len_bytes @ ..] => parse_bytes(len_bytes).map_err(|_| {
                        ConnectionError::Protocol("invalid multibulk length".to_owned())
                    })?, // array
                    _ => {
                        return Err(ConnectionError::Protocol(
                            "inline protocol is not implemented".to_owned(),
                        ))
                    }
                };
                src.advance(end + 2); // 2 for skipping \r\n
                if len <= 0 {
                    return Ok(Some(Vec::new()));
                }

                let array_len = len as usize;
                self.array_len = Some(array_len);
                assert!(self.array.is_empty());
                self.array.reserve(array_len);
                array_len
            }
            Some(array_len) => array_len,
        };

        for _ in self.array.len()..array_len {
            let end = match src.find_byte(b'\r') {
                Some(end) if end + 1 < src.len() => end,
                _ => return Ok(None),
            };
            // expect bulk string
            let [b'$', len_bytes @ ..] = &src[..end] else {
                return Err(ConnectionError::Protocol(format!(
                    "expected '$', got '{}'",
                    char::from(src[0])
                )));
            };
            let len: usize = parse_bytes(len_bytes)
                .map_err(|_| ConnectionError::Protocol("invalid bulk length".to_owned()))?;
            if end + 2 + len + 2 > src.len() {
                return Ok(None);
            }
            src.advance(end + 2);
            self.array.push(src.split_to(len).freeze());
            src.advance(2);
        }

        self.array_len = None;
        Ok(Some(std::mem::take(&mut self.array)))
    }
}

impl Encoder<&RedisResult> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: &RedisResult, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode(&mut dst.writer(), item)
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
