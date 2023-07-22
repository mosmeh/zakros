use super::{ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    error::Error,
    object::{BytesExt, RedisObject},
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};
use std::collections::{hash_map::Entry, VecDeque};

impl ReadCommandHandler for command::LIndex {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, index] = args else {
            return Err(Error::WrongArity);
        };
        let dict = dict.read();
        let list = match dict.get(key) {
            Some(RedisObject::List(list)) => list,
            Some(_) => return Err(Error::WrongType),
            None => return Ok(RedisValue::Null),
        };
        let mut index = index.to_i32()?;
        if index < 0 {
            index += list.len() as i32;
        }
        if let Ok(index) = index.try_into() {
            if let Some(element) = list.get(index) {
                return Ok(element.clone().into());
            }
        }
        Ok(RedisValue::Null)
    }
}

impl ReadCommandHandler for command::LLen {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::List(list)) => Ok((list.len() as i64).into()),
            Some(_) => Err(Error::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl WriteCommandHandler for command::LPop {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        pop(dict, args, VecDeque::pop_front)
    }
}

impl WriteCommandHandler for command::LPush {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        push::<_, _, false>(dict, args, VecDeque::push_front)
    }
}

impl WriteCommandHandler for command::LPushX {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        push::<_, _, true>(dict, args, VecDeque::push_front)
    }
}

impl ReadCommandHandler for command::LRange {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, start, stop] = args else {
            return Err(Error::WrongArity);
        };
        let mut start = start.to_i32()?;
        let mut stop = stop.to_i32()?;
        let dict = dict.read();
        let list = match dict.get(key) {
            Some(RedisObject::List(list)) => list,
            Some(_) => return Err(Error::WrongType),
            None => return Ok(RedisValue::Null),
        };
        let len = list.len() as i32;
        if start < 0 {
            start = (start + len).max(0);
        }
        if stop < 0 {
            stop += len;
        }
        if start > stop || start >= len {
            return Ok(RedisValue::Array(Vec::new()));
        }
        stop = stop.min(len - 1);
        let values = list
            .iter()
            .skip(start as usize)
            .take((stop - start) as usize + 1)
            .map(|value| value.clone().into())
            .collect();
        Ok(RedisValue::Array(values))
    }
}

impl WriteCommandHandler for command::RPop {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        pop(dict, args, VecDeque::pop_back)
    }
}

impl WriteCommandHandler for command::RPush {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        push::<_, _, false>(dict, args, VecDeque::push_back)
    }
}

impl WriteCommandHandler for command::RPushX {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        push::<_, _, true>(dict, args, VecDeque::push_back)
    }
}

impl WriteCommandHandler for command::LSet {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, index, element] = args else {
            return Err(Error::WrongArity);
        };
        let mut dict = dict.write();
        let list = match dict.get_mut(key) {
            Some(RedisObject::List(list)) => list,
            Some(_) => return Err(Error::WrongType),
            None => return Err(Error::NoKey),
        };
        let mut index = index.to_i32()?;
        if index < 0 {
            index += list.len() as i32;
        }
        if let Ok(index) = index.try_into() {
            if let Some(e) = list.get_mut(index) {
                *e = element.clone();
                return Ok(RedisValue::ok());
            }
        }
        Err(Error::IndexOutOfRange)
    }
}

impl WriteCommandHandler for command::LTrim {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, start, stop] = args else {
            return Err(Error::WrongArity);
        };
        let mut start = start.to_i32()?;
        let mut stop = stop.to_i32()?;
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let RedisObject::List(list) = entry.get_mut() else {
                    return Err(Error::WrongType);
                };
                let len = list.len() as i32;
                if start < 0 {
                    start = (start + len).max(0);
                }
                if stop < 0 {
                    stop += len;
                }
                if start > stop || start >= len {
                    entry.remove();
                } else {
                    list.truncate(stop as usize + 1);
                    list.drain(..start as usize);
                }
                Ok(RedisValue::ok())
            }
            Entry::Vacant(_) => Ok(RedisValue::ok()),
        }
    }
}

fn push<'a, D, F, const XX: bool>(dict: &'a D, args: &[Vec<u8>], f: F) -> RedisResult
where
    D: RwLockable<'a, Dictionary>,
    F: Fn(&mut VecDeque<Vec<u8>>, Vec<u8>),
{
    let (key, elements) = match args {
        [_key] => return Err(Error::WrongArity),
        [key, elements @ ..] => (key, elements),
        _ => return Err(Error::WrongArity),
    };
    match dict.write().entry(key.clone()) {
        Entry::Occupied(mut entry) => {
            let RedisObject::List(list) = entry.get_mut() else {
                return Err(Error::WrongType);
            };
            list.reserve(elements.len());
            for element in elements {
                f(list, element.clone());
            }
            Ok((list.len() as i64).into())
        }
        Entry::Vacant(entry) => {
            if XX {
                Ok(0.into())
            } else {
                let mut list = VecDeque::with_capacity(elements.len());
                for element in elements {
                    f(&mut list, element.clone());
                }
                entry.insert(RedisObject::List(list));
                Ok((elements.len() as i64).into())
            }
        }
    }
}

fn pop<'a, D, F>(dict: &'a D, args: &[Vec<u8>], f: F) -> RedisResult
where
    D: RwLockable<'a, Dictionary>,
    F: Fn(&mut VecDeque<Vec<u8>>) -> Option<Vec<u8>>,
{
    match args {
        [key] => match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let RedisObject::List(list) = entry.get_mut() else {
                    return Err(Error::WrongType);
                };
                let Some(value) = f(list) else {
                    return Ok(RedisValue::Null);
                };
                if list.is_empty() {
                    entry.remove();
                }
                Ok(value.into())
            }
            Entry::Vacant(_) => Ok(RedisValue::Null),
        },
        [key, count] => {
            let count = count.to_u32()?;
            match dict.write().entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    let RedisObject::List(list) = entry.get_mut() else {
                        return Err(Error::WrongType);
                    };
                    let mut values = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        match f(list) {
                            Some(value) => values.push(value.into()),
                            None => break,
                        }
                    }
                    if list.is_empty() {
                        entry.remove();
                    }
                    Ok(RedisValue::Array(values))
                }
                Entry::Vacant(_) => Ok(RedisValue::Null),
            }
        }
        _ => Err(Error::WrongArity),
    }
}
