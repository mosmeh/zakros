use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    error::Error,
    object::RedisObject,
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};
use std::collections::{hash_map::Entry, HashMap};

impl CommandSpec for command::HDel {
    const NAME: &'static str = "HDEL";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::HDel {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let (key, fields) = match args {
            [_key] => return Err(Error::WrongArity),
            [key, fields @ ..] => (key, fields),
            _ => return Err(Error::WrongArity),
        };
        let mut dict = dict.write();
        let entry = dict.entry(key.clone());
        let Entry::Occupied(mut entry) = entry else {
            return Ok(0.into());
        };
        let RedisObject::Hash(hash) = entry.get_mut() else {
            return Err(Error::WrongType);
        };
        let mut num_removed = 0;
        for field in fields {
            if hash.remove(field).is_some() {
                num_removed += 1;
            }
        }
        Ok(num_removed.into())
    }
}

impl CommandSpec for command::HExists {
    const NAME: &'static str = "HEXISTS";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::HExists {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, field] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::Hash(hash)) => {
                Ok(if hash.contains_key(field) { 1 } else { 0 }.into())
            }
            Some(_) => Err(Error::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::HGet {
    const NAME: &'static str = "HGET";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::HGet {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, field] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::Hash(hash)) => match hash.get(field) {
                Some(value) => Ok(value.clone().into()),
                None => Ok(RedisValue::Null),
            },
            Some(_) => Err(Error::WrongType),
            None => Ok(RedisValue::Null),
        }
    }
}

impl CommandSpec for command::HGetAll {
    const NAME: &'static str = "HGETALL";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HGetAll {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        read_hash(dict, args, |hash| {
            let mut responses = Vec::with_capacity(hash.len() * 2);
            for (field, value) in hash {
                responses.push(field.clone().into());
                responses.push(value.clone().into());
            }
            RedisValue::Array(responses)
        })
    }
}

impl CommandSpec for command::HKeys {
    const NAME: &'static str = "HKEYS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HKeys {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        read_hash(dict, args, |hash| {
            hash.keys()
                .map(|field| field.clone().into())
                .collect::<Vec<RedisValue>>()
                .into()
        })
    }
}

impl CommandSpec for command::HLen {
    const NAME: &'static str = "HLEN";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HLen {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        read_hash(dict, args, |hash| (hash.len() as i64).into())
    }
}

impl CommandSpec for command::HMGet {
    const NAME: &'static str = "HMGET";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl ReadCommandHandler for command::HMGet {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let (key, fields) = match args {
            [_key] => return Err(Error::WrongArity),
            [key, fields @ ..] => (key, fields),
            _ => return Err(Error::WrongArity),
        };
        match dict.read().get(key) {
            Some(RedisObject::Hash(hash)) => {
                let values = fields
                    .iter()
                    .map(|field| match hash.get(field) {
                        Some(value) => value.clone().into(),
                        None => RedisValue::Null,
                    })
                    .collect();
                Ok(RedisValue::Array(values))
            }
            Some(_) => Err(Error::WrongType),
            None => Ok(vec![RedisValue::Null; fields.len()].into()),
        }
    }
}

impl CommandSpec for command::HSet {
    const NAME: &'static str = "HSET";
    const ARITY: Arity = Arity::AtLeast(3);
}

impl WriteCommandHandler for command::HSet {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let (key, pairs) = match args {
            [_key] => return Err(Error::WrongArity),
            [key, pairs @ ..] if pairs.len() % 2 == 0 => (key, pairs),
            _ => return Err(Error::WrongArity),
        };
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let RedisObject::Hash(hash) = entry.get_mut() else {
                    return Err(Error::WrongType);
                };
                let mut num_added = 0;
                for pair in pairs.chunks_exact(2) {
                    let [field, value] = pair else { unreachable!() };
                    if hash.insert(field.clone(), value.clone()).is_none() {
                        num_added += 1;
                    }
                }
                Ok(num_added.into())
            }
            Entry::Vacant(entry) => {
                let len = (pairs.len() / 2) as i64;
                let pairs = pairs.chunks_exact(2).map(|pair| {
                    let [field, value] = pair else { unreachable!() };
                    (field.clone(), value.clone())
                });
                entry.insert(RedisObject::Hash(HashMap::from_iter(pairs)));
                Ok(len.into())
            }
        }
    }
}

impl CommandSpec for command::HSetNx {
    const NAME: &'static str = "HSETNX";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::HSetNx {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, field, value] = args else {
            return Err(Error::WrongArity);
        };
        let was_set = match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let RedisObject::Hash(hash) = entry.get_mut() else {
                    return Err(Error::WrongType);
                };
                match hash.entry(field.clone()) {
                    Entry::Occupied(_) => false,
                    Entry::Vacant(entry) => {
                        entry.insert(value.clone());
                        true
                    }
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(RedisObject::Hash(HashMap::from([(
                    field.clone(),
                    value.clone(),
                )])));
                true
            }
        };
        Ok(if was_set { 1 } else { 0 }.into())
    }
}

impl CommandSpec for command::HVals {
    const NAME: &'static str = "HVALS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HVals {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        read_hash(dict, args, |hash| {
            hash.values()
                .map(|field| field.clone().into())
                .collect::<Vec<RedisValue>>()
                .into()
        })
    }
}

fn read_hash<'a, D, F>(dict: &'a D, args: &[Vec<u8>], f: F) -> RedisResult
where
    D: ReadLockable<'a, Dictionary>,
    F: Fn(&HashMap<Vec<u8>, Vec<u8>>) -> RedisValue,
{
    let [key] = args else {
        return Err(Error::WrongArity);
    };
    match dict.read().get(key) {
        Some(RedisObject::Hash(hash)) => Ok(f(hash)),
        Some(_) => Err(Error::WrongType),
        None => Ok(RedisValue::Array(Vec::new())),
    }
}
