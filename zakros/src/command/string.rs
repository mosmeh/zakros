use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    error::Error,
    object::{BytesExt, RedisObject},
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};
use std::collections::hash_map::Entry;

impl CommandSpec for command::Append {
    const NAME: &'static str = "APPEND";
    const ARITY: Arity = Arity::Fixed(2);
}

impl WriteCommandHandler for command::Append {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, value] = args else {
            return Err(Error::WrongArity);
        };
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let RedisObject::String(s) = entry.get_mut() else {
                    return Err(Error::WrongType);
                };
                s.extend_from_slice(value);
                Ok((s.len() as i64).into())
            }
            Entry::Vacant(entry) => {
                let len = value.len();
                entry.insert(value.clone().into());
                Ok((len as i64).into())
            }
        }
    }
}

impl CommandSpec for command::Get {
    const NAME: &'static str = "GET";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::Get {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::String(value)) => Ok(value.clone().into()),
            Some(_) => Err(Error::WrongType),
            None => Ok(RedisValue::Null),
        }
    }
}

impl CommandSpec for command::GetRange {
    const NAME: &'static str = "GETRANGE";
    const ARITY: Arity = Arity::Fixed(3);
}

impl ReadCommandHandler for command::GetRange {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, start, end] = args else {
            return Err(Error::WrongArity);
        };
        let mut start = start.to_i64()?;
        let mut end = end.to_i64()?;
        if start < 0 && end < 0 && start > end {
            return Ok(RedisValue::BulkString(Vec::new()));
        }
        match dict.read().get(key) {
            Some(RedisObject::String(s)) => {
                let len = s.len() as i64;
                if start < 0 {
                    start = (start + len).max(0);
                }
                if end < 0 {
                    end = (end + len).max(0);
                }
                end = end.min(len - 1);
                Ok(if start > end || s.is_empty() {
                    RedisValue::BulkString(Vec::new())
                } else {
                    s[start as usize..=end as usize].to_vec().into()
                })
            }
            Some(_) => Err(Error::WrongType),
            None => Ok(RedisValue::BulkString(Vec::new())),
        }
    }
}

impl CommandSpec for command::MGet {
    const NAME: &'static str = "MGET";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::MGet {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            return Err(Error::WrongArity);
        }
        let dict = dict.read();
        let values = args
            .iter()
            .map(|key| match dict.get(key) {
                Some(RedisObject::String(value)) => value.clone().into(),
                _ => RedisValue::Null,
            })
            .collect();
        Ok(RedisValue::Array(values))
    }
}

impl CommandSpec for command::MSet {
    const NAME: &'static str = "MSET";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::MSet {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() || args.len() % 2 != 0 {
            return Err(Error::WrongArity);
        }
        let mut dict = dict.write();
        for pair in args.chunks_exact(2) {
            let [key, value] = pair else { unreachable!() };
            dict.insert(key.clone(), value.clone().into());
        }
        Ok(RedisValue::ok())
    }
}

impl CommandSpec for command::MSetNx {
    const NAME: &'static str = "MSETNX";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::MSetNx {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() || args.len() % 2 != 0 {
            return Err(Error::WrongArity);
        }
        let mut dict = dict.write();
        for key in args.iter().step_by(2) {
            if dict.contains_key(key) {
                return Ok(0.into());
            }
        }
        for pair in args.chunks_exact(2) {
            let [key, value] = pair else { unreachable!() };
            dict.insert(key.clone(), value.clone().into());
        }
        Ok(1.into())
    }
}

impl CommandSpec for command::Set {
    const NAME: &'static str = "SET";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::Set {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, value, options @ ..] = args else {
            return Err(Error::WrongArity);
        };
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        for option in options {
            match option.to_ascii_uppercase().as_slice() {
                b"NX" => nx = true,
                b"XX" => xx = true,
                b"GET" => get = true,
                _ => return Err(Error::SyntaxError),
            }
        }
        if nx && xx {
            return Err(Error::SyntaxError);
        }
        let value = value.clone().into();
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if get {
                    let RedisObject::String(s) = entry.get() else {
                        return Err(Error::WrongType);
                    };
                    let original_value = s.clone();
                    if !nx {
                        entry.insert(value);
                    }
                    return Ok(original_value.into());
                }
                Ok(if !nx {
                    entry.insert(value);
                    RedisValue::ok()
                } else {
                    RedisValue::Null
                })
            }
            Entry::Vacant(entry) => {
                if !xx {
                    entry.insert(value);
                }
                Ok(if get || xx {
                    RedisValue::Null
                } else {
                    RedisValue::ok()
                })
            }
        }
    }
}

impl CommandSpec for command::SetRange {
    const NAME: &'static str = "SETRANGE";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::SetRange {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, offset, value] = args else {
            return Err(Error::WrongArity);
        };
        let offset = offset
            .to_i32()?
            .try_into()
            .map_err(|_| Error::ValueOutOfRange)?;
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let RedisObject::String(s) = entry.get_mut() else {
                    return Err(Error::WrongType);
                };
                let end = offset + value.len();
                if end > s.len() {
                    s.resize(end, 0);
                }
                s[offset..end].copy_from_slice(value);
                Ok((s.len() as i64).into())
            }
            Entry::Vacant(entry) => {
                let len = offset + value.len();
                let mut s = Vec::with_capacity(len);
                s.resize(offset, 0);
                s.extend_from_slice(value);
                entry.insert(s.into());
                Ok((len as i64).into())
            }
        }
    }
}

impl CommandSpec for command::StrLen {
    const NAME: &'static str = "STRLEN";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::StrLen {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::String(s)) => Ok((s.len() as i64).into()),
            Some(_) => Err(Error::WrongType),
            None => Ok(0.into()),
        }
    }
}
