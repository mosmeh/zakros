use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    error::Error,
    object::RedisObject,
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};
use std::collections::{hash_map::Entry, HashSet};

impl CommandSpec for command::SAdd {
    const NAME: &'static str = "SADD";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SAdd {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let (key, members) = match args {
            [_key] => return Err(Error::WrongArity),
            [key, members @ ..] => (key, members),
            _ => return Err(Error::WrongArity),
        };
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let RedisObject::Set(set) = entry.get_mut() else {
                    return Err(Error::WrongType);
                };
                set.reserve(members.len());
                let mut num_inserted = 0;
                for member in members {
                    if set.insert(member.clone()) {
                        num_inserted += 1;
                    }
                }
                Ok(num_inserted.into())
            }
            Entry::Vacant(entry) => {
                entry.insert(RedisObject::Set(HashSet::from_iter(
                    members.iter().cloned(),
                )));
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
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::Set(set)) => Ok((set.len() as i64).into()),
            Some(_) => Err(Error::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::SDiff {
    const NAME: &'static str = "SDIFF";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::SDiff {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        apply_and_return(dict, args, diff)
    }
}

impl CommandSpec for command::SDiffStore {
    const NAME: &'static str = "SDIFFSTORE";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SDiffStore {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        apply_and_store(dict, args, diff)
    }
}

impl CommandSpec for command::SInter {
    const NAME: &'static str = "SINTER";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::SInter {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        apply_and_return(dict, args, intersection)
    }
}

impl CommandSpec for command::SInterStore {
    const NAME: &'static str = "SINTERSTORE";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SInterStore {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        apply_and_store(dict, args, intersection)
    }
}

impl CommandSpec for command::SIsMember {
    const NAME: &'static str = "SISMEMBER";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::SIsMember {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, member] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::Set(set)) => Ok((if set.contains(member) { 1 } else { 0 }).into()),
            Some(_) => Err(Error::WrongType),
            None => Ok(0.into()),
        }
    }
}

impl CommandSpec for command::SMembers {
    const NAME: &'static str = "SMEMBERS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::SMembers {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key] = args else {
            return Err(Error::WrongArity);
        };
        match dict.read().get(key) {
            Some(RedisObject::Set(set)) => {
                let members: Vec<RedisValue> =
                    set.iter().map(|member| member.clone().into()).collect();
                Ok(members.into())
            }
            Some(_) => Err(Error::WrongType),
            None => Ok(RedisValue::Array(Vec::new())),
        }
    }
}

impl CommandSpec for command::SMove {
    const NAME: &'static str = "SMOVE";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::SMove {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [source, destination, member] = args else {
            return Err(Error::WrongArity);
        };
        let mut dict = dict.write();
        match dict.get(destination) {
            Some(RedisObject::Set(_)) | None => (),
            Some(_) => return Err(Error::WrongType),
        }
        let source_entry = dict.entry(source.clone());
        let Entry::Occupied(mut source_entry) = source_entry else {
            return Ok(0.into());
        };
        let RedisObject::Set(source_set) = source_entry.get_mut() else {
            return Err(Error::WrongType);
        };
        if !source_set.remove(member) {
            return Ok(0.into());
        }
        match dict.entry(destination.clone()) {
            Entry::Occupied(mut dest_entry) => {
                let RedisObject::Set(dest_set) = dest_entry.get_mut() else {
                    unreachable!()
                };
                dest_set.insert(member.clone());
            }
            Entry::Vacant(dest_entry) => {
                dest_entry.insert(RedisObject::Set([member.clone()].into()));
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
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let (key, members) = match args {
            [_key] => return Err(Error::WrongArity),
            [key, members @ ..] => (key, members),
            _ => return Err(Error::WrongArity),
        };
        let mut dict = dict.write();
        let entry = dict.entry(key.clone());
        let Entry::Occupied(mut entry) = entry else {
            return Ok(0.into());
        };
        let RedisObject::Set(set) = entry.get_mut() else {
            return Err(Error::WrongType);
        };
        let mut num_removed = 0;
        for member in members {
            if set.remove(member) {
                num_removed += 1;
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
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        apply_and_return(dict, args, union)
    }
}

impl CommandSpec for command::SUnionStore {
    const NAME: &'static str = "SUNIONSTORE";
    const ARITY: Arity = Arity::AtLeast(2);
}

impl WriteCommandHandler for command::SUnionStore {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        apply_and_store(dict, args, union)
    }
}

fn apply_and_return<'a, D, F>(dict: &'a D, args: &[Vec<u8>], f: F) -> RedisResult
where
    D: ReadLockable<'a, Dictionary>,
    F: Fn(&mut HashSet<Vec<u8>>, &HashSet<Vec<u8>>),
{
    let [a_key, b_keys @ ..] = args else {
        return Err(Error::WrongArity);
    };
    let values: Vec<RedisValue> = apply(&dict.read(), a_key, b_keys, f)?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(values.into())
}

fn apply_and_store<'a, D, F>(dict: &'a D, args: &[Vec<u8>], f: F) -> RedisResult
where
    D: RwLockable<'a, Dictionary>,
    F: Fn(&mut HashSet<Vec<u8>>, &HashSet<Vec<u8>>),
{
    let [destination, a_key, b_keys @ ..] = args else {
        return Err(Error::WrongArity);
    };
    let mut dict = dict.write();
    let set = apply(&dict, a_key, b_keys, f)?;
    let len = set.len();
    dict.insert(destination.clone(), RedisObject::Set(set));
    Ok((len as i64).into())
}

fn apply<F>(
    dict: &Dictionary,
    a_key: &[u8],
    b_keys: &[Vec<u8>],
    f: F,
) -> Result<HashSet<Vec<u8>>, Error>
where
    F: Fn(&mut HashSet<Vec<u8>>, &HashSet<Vec<u8>>),
{
    let mut a_set = match dict.get(a_key) {
        Some(RedisObject::Set(set)) => set.clone(),
        Some(_) => return Err(Error::WrongType),
        None => Default::default(),
    };
    for b_key in b_keys {
        match dict.get(b_key) {
            Some(RedisObject::Set(b_set)) => f(&mut a_set, b_set),
            Some(_) => return Err(Error::WrongType),
            None => (),
        }
    }
    Ok(a_set)
}

fn diff(a: &mut HashSet<Vec<u8>>, b: &HashSet<Vec<u8>>) {
    if !a.is_empty() {
        for member in b {
            a.remove(member);
        }
    }
}

fn intersection(a: &mut HashSet<Vec<u8>>, b: &HashSet<Vec<u8>>) {
    if b.is_empty() {
        a.clear();
    } else {
        a.retain(|member| b.contains(member))
    }
}

fn union(a: &mut HashSet<Vec<u8>>, b: &HashSet<Vec<u8>>) {
    for member in b {
        a.insert(member.clone());
    }
}
