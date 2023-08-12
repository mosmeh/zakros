use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    lockable::{ReadLockable, RwLockable},
    resp::Value,
    Dictionary, Object, RedisError, RedisResult, ResponseError,
};
use bytes::Bytes;
use std::collections::{hash_map::Entry, HashSet};

impl CommandSpec for command::SAdd {
    const NAME: &'static str = "SADD";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SAdd {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let (key, members) = match args {
            [key, members @ ..] if !members.is_empty() => (key, members),
            _ => return Err(ResponseError::WrongArity.into()),
        };
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let Object::Set(set) = entry.get_mut() else {
                    return Err(RedisError::WrongType);
                };
                let mut num_inserted = 0;
                for member in members {
                    if set.insert(member.clone()) {
                        num_inserted += 1;
                    }
                }
                Ok(num_inserted.into())
            }
            Entry::Vacant(entry) => {
                entry.insert(HashSet::from_iter(members.iter().cloned()).into());
                Ok((members.len() as i64).into())
            }
        }
    }
}

impl CommandSpec for command::SCard {
    const NAME: &'static str = "SCARD";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::SCard {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.read().get(key) {
            Some(Object::Set(set)) => Ok((set.len() as i64).into()),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::SDiff {
    const NAME: &'static str = "SDIFF";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::SDiff {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        apply_and_return(dict, args, diff)
    }
}

impl CommandSpec for command::SDiffStore {
    const NAME: &'static str = "SDIFFSTORE";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SDiffStore {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        apply_and_store(dict, args, diff)
    }
}

impl CommandSpec for command::SInter {
    const NAME: &'static str = "SINTER";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::SInter {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        apply_and_return(dict, args, intersection)
    }
}

impl CommandSpec for command::SInterStore {
    const NAME: &'static str = "SINTERSTORE";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SInterStore {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        apply_and_store(dict, args, intersection)
    }
}

impl CommandSpec for command::SIsMember {
    const NAME: &'static str = "SISMEMBER";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::SIsMember {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, member] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.read().get(key) {
            Some(Object::Set(set)) => Ok((set.contains(member) as i64).into()),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::SMembers {
    const NAME: &'static str = "SMEMBERS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::SMembers {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.read().get(key) {
            Some(Object::Set(set)) => {
                let members: Vec<_> = set.iter().map(|member| Ok(member.clone().into())).collect();
                Ok(members.into())
            }
            Some(_) => Err(RedisError::WrongType),
            None => Ok(Value::Array(Vec::new())),
        }
    }
}

impl CommandSpec for command::SMIsMember {
    const NAME: &'static str = "SMISMEMBER";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl ReadCommandHandler for command::SMIsMember {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let (key, members) = match args {
            [key, members @ ..] if !members.is_empty() => (key, members),
            _ => return Err(ResponseError::WrongArity.into()),
        };
        let responses = match dict.read().get(key) {
            Some(hash) => {
                let Object::Set(hash) = hash else {
                    return Err(RedisError::WrongType);
                };
                members
                    .iter()
                    .map(|member| Ok((hash.contains(member) as i64).into()))
                    .collect()
            }
            None => vec![Ok(0.into()); members.len()],
        };
        Ok(Value::Array(responses))
    }
}

impl CommandSpec for command::SMove {
    const NAME: &'static str = "SMOVE";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::SMove {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [source, destination, member] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut dict = dict.write();
        match dict.get(destination) {
            Some(Object::Set(_)) | None => (),
            Some(_) => return Err(RedisError::WrongType),
        }
        let source_entry = dict.entry(source.clone());
        let Entry::Occupied(mut source_entry) = source_entry else {
            return Ok(0.into());
        };
        let Object::Set(source_set) = source_entry.get_mut() else {
            return Err(RedisError::WrongType);
        };
        if !source_set.remove(member) {
            return Ok(0.into());
        }
        if source_set.is_empty() {
            source_entry.remove();
        }
        match dict.entry(destination.clone()) {
            Entry::Occupied(mut dest_entry) => {
                let Object::Set(dest_set) = dest_entry.get_mut() else {
                    unreachable!()
                };
                dest_set.insert(member.clone());
            }
            Entry::Vacant(dest_entry) => {
                dest_entry.insert(Object::Set([member.clone()].into()));
            }
        }
        Ok(1.into())
    }
}

impl CommandSpec for command::SRem {
    const NAME: &'static str = "SREM";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SRem {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let (key, members) = match args {
            [key, members @ ..] if !members.is_empty() => (key, members),
            _ => return Err(ResponseError::WrongArity.into()),
        };
        let mut dict = dict.write();
        let entry = dict.entry(key.clone());
        let Entry::Occupied(mut entry) = entry else {
            return Ok(0.into());
        };
        let Object::Set(set) = entry.get_mut() else {
            return Err(RedisError::WrongType);
        };
        let mut num_removed = 0;
        for member in members {
            if !set.remove(member) {
                continue;
            }
            num_removed += 1;
            if set.is_empty() {
                entry.remove();
                break;
            }
        }
        Ok(num_removed.into())
    }
}

impl CommandSpec for command::SUnion {
    const NAME: &'static str = "SUNION";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::SUnion {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        apply_and_return(dict, args, union)
    }
}

impl CommandSpec for command::SUnionStore {
    const NAME: &'static str = "SUNIONSTORE";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SUnionStore {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        apply_and_store(dict, args, union)
    }
}

fn apply_and_return<'a, D, F>(dict: &'a D, args: &[Bytes], f: F) -> RedisResult
where
    D: ReadLockable<'a, Dictionary>,
    F: Fn(&mut HashSet<Bytes>, &HashSet<Bytes>),
{
    let [lhs_key, rhs_keys @ ..] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    let values: Vec<_> = apply(&dict.read(), lhs_key, rhs_keys, f)?
        .into_iter()
        .map(|value| Ok(value.into()))
        .collect();
    Ok(values.into())
}

fn apply_and_store<'a, D, F>(dict: &'a D, args: &[Bytes], f: F) -> RedisResult
where
    D: RwLockable<'a, Dictionary>,
    F: Fn(&mut HashSet<Bytes>, &HashSet<Bytes>),
{
    let [destination, lfs_key, rhs_keys @ ..] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    let mut dict = dict.write();
    let set = apply(&dict, lfs_key, rhs_keys, f)?;
    let len = set.len();
    if len > 0 {
        dict.insert(destination.clone(), set.into());
    } else {
        dict.remove(destination);
    }
    Ok((len as i64).into())
}

fn apply<F>(
    dict: &Dictionary,
    lhs_key: &[u8],
    rhs_keys: &[Bytes],
    f: F,
) -> Result<HashSet<Bytes>, RedisError>
where
    F: Fn(&mut HashSet<Bytes>, &HashSet<Bytes>),
{
    let mut lhs_set = match dict.get(lhs_key) {
        Some(Object::Set(set)) => set.clone(),
        Some(_) => return Err(RedisError::WrongType),
        None => Default::default(),
    };
    for rhs_key in rhs_keys {
        match dict.get(rhs_key) {
            Some(Object::Set(rhs_set)) => f(&mut lhs_set, rhs_set),
            Some(_) => return Err(RedisError::WrongType),
            None => f(&mut lhs_set, &HashSet::new()),
        }
    }
    Ok(lhs_set)
}

fn diff(dest: &mut HashSet<Bytes>, src: &HashSet<Bytes>) {
    for member in src {
        if dest.is_empty() {
            break;
        }
        dest.remove(member);
    }
}

fn intersection(dest: &mut HashSet<Bytes>, src: &HashSet<Bytes>) {
    if src.is_empty() {
        dest.clear();
    } else {
        dest.retain(|member| src.contains(member))
    }
}

fn union(dest: &mut HashSet<Bytes>, src: &HashSet<Bytes>) {
    for member in src {
        dest.insert(member.clone());
    }
}
