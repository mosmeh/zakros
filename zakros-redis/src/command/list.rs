use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    lockable::{ReadLockable, RwLockable},
    resp::Value,
    BytesExt, Dictionary, Object, RedisError, RedisResult, ResponseError,
};
use bytes::Bytes;
use std::collections::{hash_map::Entry, VecDeque};

impl CommandSpec for command::LIndex {
    const NAME: &'static str = "LINDEX";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::LIndex {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, index] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let dict = dict.read();
        let list = match dict.get(key) {
            Some(Object::List(list)) => list,
            Some(_) => return Err(RedisError::WrongType),
            None => return Ok(Value::Null),
        };
        let mut index = index.to_i64()?;
        if index < 0 {
            index += list.len() as i64;
        }
        if let Ok(index) = index.try_into() {
            if let Some(element) = list.get(index) {
                return Ok(element.clone().into());
            }
        }
        Ok(Value::Null)
    }
}

impl CommandSpec for command::LLen {
    const NAME: &'static str = "LLEN";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::LLen {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.read().get(key) {
            Some(Object::List(list)) => Ok((list.len() as i64).into()),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::LPop {
    const NAME: &'static str = "LPOP";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::LPop {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        pop(dict, args, VecDeque::pop_front)
    }
}

impl CommandSpec for command::LPush {
    const NAME: &'static str = "LPUSH";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::LPush {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        push::<_, _, false>(dict, args, VecDeque::push_front)
    }
}

impl CommandSpec for command::LPushX {
    const NAME: &'static str = "LPUSHX";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::LPushX {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        push::<_, _, true>(dict, args, VecDeque::push_front)
    }
}

impl CommandSpec for command::LRange {
    const NAME: &'static str = "LRANGE";
    const ARITY: Arity = Arity::Fixed(3);
}

impl ReadCommandHandler for command::LRange {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, start, stop] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut start = start.to_i64()?;
        let mut stop = stop.to_i64()?;
        let dict = dict.read();
        let list = match dict.get(key) {
            Some(Object::List(list)) => list,
            Some(_) => return Err(RedisError::WrongType),
            None => return Ok(Value::Null),
        };
        let len = list.len() as i64;
        if start < 0 {
            start = (start + len).max(0);
        }
        if stop < 0 {
            stop += len;
        }
        if start > stop || start >= len {
            return Ok(Value::Array(Vec::new()));
        }
        stop = stop.min(len - 1);
        let values = list
            .iter()
            .skip(start as usize)
            .take((stop - start) as usize + 1)
            .map(|value| Ok(value.clone().into()))
            .collect();
        Ok(Value::Array(values))
    }
}

impl CommandSpec for command::LSet {
    const NAME: &'static str = "LSET";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::LSet {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, index, element] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut dict = dict.write();
        let list = match dict.get_mut(key) {
            Some(Object::List(list)) => list,
            Some(_) => return Err(RedisError::WrongType),
            None => return Err(ResponseError::NoKey.into()),
        };
        let mut index = index.to_i64()?;
        if index < 0 {
            index += list.len() as i64;
        }
        if let Ok(index) = index.try_into() {
            if let Some(e) = list.get_mut(index) {
                *e = element.clone();
                return Ok(Value::ok());
            }
        }
        Err(ResponseError::IndexOutOfRange.into())
    }
}

impl CommandSpec for command::LTrim {
    const NAME: &'static str = "LTRIM";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::LTrim {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, start, stop] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut start = start.to_i64()?;
        let mut stop = stop.to_i64()?;
        let mut dict = dict.write();
        let Entry::Occupied(mut entry) = dict.entry(key.clone()) else {
            return Ok(Value::ok());
        };
        let Object::List(list) = entry.get_mut() else {
            return Err(RedisError::WrongType);
        };
        let len = list.len() as i64;
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
        Ok(Value::ok())
    }
}

impl CommandSpec for command::RPop {
    const NAME: &'static str = "RPOP";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::RPop {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        pop(dict, args, VecDeque::pop_back)
    }
}

impl CommandSpec for command::RPopLPush {
    const NAME: &'static str = "RPOPLPUSH";
    const ARITY: Arity = Arity::Fixed(2);
}

impl WriteCommandHandler for command::RPopLPush {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [source, destination] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut dict = dict.write();
        match dict.get(destination) {
            Some(Object::List(_)) | None => (),
            Some(_) => return Err(RedisError::WrongType),
        }
        let source_entry = dict.entry(source.clone());
        let Entry::Occupied(mut source_entry) = source_entry else {
            return Ok(Value::Null);
        };
        let Object::List(source_list) = source_entry.get_mut() else {
            return Err(RedisError::WrongType);
        };
        let Some(value) = source_list.pop_back() else {
            unreachable!()
        };
        if source_list.is_empty() {
            source_entry.remove();
        }
        match dict.entry(destination.clone()) {
            Entry::Occupied(dest_entry) => {
                let Object::List(dest_list) = dest_entry.into_mut() else {
                    unreachable!()
                };
                dest_list.push_front(value.clone());
            }
            Entry::Vacant(dest_entry) => {
                dest_entry.insert(Object::List([value.clone()].into()));
            }
        }
        Ok(value.into())
    }
}

impl CommandSpec for command::RPush {
    const NAME: &'static str = "RPUSH";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::RPush {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        push::<_, _, false>(dict, args, VecDeque::push_back)
    }
}

impl CommandSpec for command::RPushX {
    const NAME: &'static str = "RPUSHX";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::RPushX {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        push::<_, _, true>(dict, args, VecDeque::push_back)
    }
}

fn push<'a, D, F, const XX: bool>(dict: &'a D, args: &[Bytes], f: F) -> RedisResult
where
    D: RwLockable<'a, Dictionary>,
    F: Fn(&mut VecDeque<Bytes>, Bytes),
{
    let (key, elements) = match args {
        [key, elements @ ..] if !elements.is_empty() => (key, elements),
        _ => return Err(ResponseError::WrongArity.into()),
    };
    match dict.write().entry(key.clone()) {
        Entry::Occupied(entry) => {
            let Object::List(list) = entry.into_mut() else {
                return Err(RedisError::WrongType);
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
                entry.insert(Object::List(list));
                Ok((elements.len() as i64).into())
            }
        }
    }
}

fn pop<'a, D, F>(dict: &'a D, args: &[Bytes], f: F) -> RedisResult
where
    D: RwLockable<'a, Dictionary>,
    F: Fn(&mut VecDeque<Bytes>) -> Option<Bytes>,
{
    match args {
        [key] => match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let Object::List(list) = entry.get_mut() else {
                    return Err(RedisError::WrongType);
                };
                let Some(value) = f(list) else {
                    return Ok(Value::Null);
                };
                if list.is_empty() {
                    entry.remove();
                }
                Ok(value.into())
            }
            Entry::Vacant(_) => Ok(Value::Null),
        },
        [key, count] => {
            let count = count.to_u64()?;
            match dict.write().entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    let Object::List(list) = entry.get_mut() else {
                        return Err(RedisError::WrongType);
                    };
                    let mut values = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        match f(list) {
                            Some(value) => values.push(Ok(value.into())),
                            None => break,
                        }
                    }
                    if list.is_empty() {
                        entry.remove();
                    }
                    Ok(Value::Array(values))
                }
                Entry::Vacant(_) => Ok(Value::Null),
            }
        }
        _ => Err(ResponseError::WrongArity.into()),
    }
}
