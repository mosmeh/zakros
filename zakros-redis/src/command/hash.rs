use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    lockable::{ReadLockable, RwLockable},
    resp::Value,
    BytesExt, Dictionary, Object, RedisError, RedisResult, ResponseError,
};
use bytes::Bytes;
use std::collections::{hash_map::Entry, HashMap};

impl CommandSpec for command::HDel {
    const NAME: &'static str = "HDEL";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::HDel {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let (key, fields) = match args {
            [key, fields @ ..] if !fields.is_empty() => (key, fields),
            _ => return Err(ResponseError::WrongArity.into()),
        };
        let mut dict = dict.write();
        let entry = dict.entry(key.clone());
        let Entry::Occupied(mut entry) = entry else {
            return Ok(0.into());
        };
        let Object::Hash(hash) = entry.get_mut() else {
            return Err(RedisError::WrongType);
        };
        let mut num_removed = 0;
        for field in fields {
            if hash.remove(field).is_none() {
                continue;
            }
            num_removed += 1;
            if hash.is_empty() {
                entry.remove();
                break;
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
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, field] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.read().get(key) {
            Some(Object::Hash(hash)) => Ok((hash.contains_key(field) as i64).into()),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::HGet {
    const NAME: &'static str = "HGET";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::HGet {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, field] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.read().get(key) {
            Some(Object::Hash(hash)) => match hash.get(field) {
                Some(value) => Ok(value.clone().into()),
                None => Ok(Value::Null),
            },
            Some(_) => Err(RedisError::WrongType),
            None => Ok(Value::Null),
        }
    }
}

impl CommandSpec for command::HGetAll {
    const NAME: &'static str = "HGETALL";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HGetAll {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        read_hash(dict, args, |hash| {
            let mut responses = Vec::with_capacity(hash.len() * 2);
            for (field, value) in hash {
                responses.push(Ok(field.clone().into()));
                responses.push(Ok(value.clone().into()));
            }
            Value::Array(responses)
        })
    }
}

impl CommandSpec for command::HIncrBy {
    const NAME: &'static str = "HINCRBY";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::HIncrBy {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, field, increment] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let increment = increment.to_i64()?;
        match dict.write().entry(key.clone()) {
            Entry::Occupied(key_entry) => {
                let Object::Hash(hash) = key_entry.into_mut() else {
                    return Err(RedisError::WrongType);
                };
                match hash.entry(field.clone()) {
                    Entry::Occupied(mut field_entry) => {
                        let value = field_entry.get().to_i64().map_err(|_| {
                            RedisError::from(ResponseError::Other("hash value is not an integer"))
                        })?;
                        let new_value = value.checked_add(increment).ok_or_else(|| {
                            RedisError::from(ResponseError::Other(
                                "increment or decrement would overflow",
                            ))
                        })?;
                        field_entry.insert(new_value.to_string().into_bytes().into());
                        Ok(new_value.into())
                    }
                    Entry::Vacant(field_entry) => {
                        field_entry.insert(increment.to_string().into_bytes().into());
                        Ok(increment.into())
                    }
                }
            }
            Entry::Vacant(key_entry) => {
                key_entry.insert(
                    HashMap::from([(field.clone(), increment.to_string().into_bytes().into())])
                        .into(),
                );
                Ok(increment.into())
            }
        }
    }
}

impl CommandSpec for command::HKeys {
    const NAME: &'static str = "HKEYS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HKeys {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        read_hash(dict, args, |hash| {
            hash.keys()
                .map(|field| Ok(field.clone().into()))
                .collect::<Vec<_>>()
                .into()
        })
    }
}

impl CommandSpec for command::HLen {
    const NAME: &'static str = "HLEN";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HLen {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        read_hash(dict, args, |hash| (hash.len() as i64).into())
    }
}

impl CommandSpec for command::HMGet {
    const NAME: &'static str = "HMGET";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl ReadCommandHandler for command::HMGet {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let (key, fields) = match args {
            [key, fields @ ..] if !fields.is_empty() => (key, fields),
            _ => return Err(ResponseError::WrongArity.into()),
        };
        match dict.read().get(key) {
            Some(Object::Hash(hash)) => {
                let values = fields
                    .iter()
                    .map(|field| {
                        Ok(match hash.get(field) {
                            Some(value) => value.clone().into(),
                            None => Value::Null,
                        })
                    })
                    .collect();
                Ok(Value::Array(values))
            }
            Some(_) => Err(RedisError::WrongType),
            None => Ok(vec![Ok(Value::Null); fields.len()].into()),
        }
    }
}

impl CommandSpec for command::HMSet {
    const NAME: &'static str = "HMSET";
    const ARITY: Arity = Arity::AtLeast(3);
}

impl WriteCommandHandler for command::HMSet {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        command::HSet::call(dict, args)?;
        Ok(Value::ok())
    }
}

impl CommandSpec for command::HStrLen {
    const NAME: &'static str = "HSTRLEN";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::HStrLen {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, field] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.read().get(key) {
            Some(Object::Hash(hash)) => match hash.get(field) {
                Some(value) => Ok((value.len() as i64).into()),
                None => Ok(0.into()),
            },
            Some(_) => Err(RedisError::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::HSet {
    const NAME: &'static str = "HSET";
    const ARITY: Arity = Arity::AtLeast(3);
}

impl WriteCommandHandler for command::HSet {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let (key, pairs) = match args {
            [key, pairs @ ..] if !pairs.is_empty() && pairs.len() % 2 == 0 => (key, pairs),
            _ => return Err(ResponseError::WrongArity.into()),
        };
        match dict.write().entry(key.clone()) {
            Entry::Occupied(entry) => {
                let Object::Hash(hash) = entry.into_mut() else {
                    return Err(RedisError::WrongType);
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
                entry.insert(HashMap::from_iter(pairs).into());
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
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, field, value] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let was_set = match dict.write().entry(key.clone()) {
            Entry::Occupied(entry) => {
                let Object::Hash(hash) = entry.into_mut() else {
                    return Err(RedisError::WrongType);
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
                entry.insert(HashMap::from([(field.clone(), value.clone())]).into());
                true
            }
        };
        Ok((was_set as i64).into())
    }
}

impl CommandSpec for command::HVals {
    const NAME: &'static str = "HVALS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::HVals {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        read_hash(dict, args, |hash| {
            hash.values()
                .map(|field| Ok(field.clone().into()))
                .collect::<Vec<_>>()
                .into()
        })
    }
}

fn read_hash<'a, D, F>(dict: &'a D, args: &[Bytes], f: F) -> RedisResult
where
    D: ReadLockable<'a, Dictionary>,
    F: Fn(&HashMap<Bytes, Bytes>) -> Value,
{
    let [key] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    match dict.read().get(key) {
        Some(Object::Hash(hash)) => Ok(f(hash)),
        Some(_) => Err(RedisError::WrongType),
        None => Ok(Value::Array(Vec::new())),
    }
}
