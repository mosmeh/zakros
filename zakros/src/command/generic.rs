use super::{ReadCommand, WriteCommand};
use crate::{
    command,
    error::Error,
    object::RedisObject,
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};

impl WriteCommand for command::Del {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            return Err(Error::WrongArity);
        }
        let mut num_deleted = 0;
        let mut dict = dict.write();
        for key in args {
            if dict.remove(key).is_some() {
                num_deleted += 1;
            }
        }
        Ok(num_deleted.into())
    }
}

impl ReadCommand for command::Exists {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            return Err(Error::WrongArity);
        }
        let mut num_exists = 0;
        let dict = dict.read();
        for key in args {
            if dict.contains_key(key) {
                num_exists += 1;
            }
        }
        Ok(num_exists.into())
    }
}

impl ReadCommand for command::Keys {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [pattern] = args else {
            return Err(Error::WrongArity);
        };
        let keys = dict
            .read()
            .keys()
            .filter(|key| crate::string::string_matches(pattern, key))
            .map(|key| key.clone().into())
            .collect();
        Ok(RedisValue::Array(keys))
    }
}

impl WriteCommand for command::Rename {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, new_key] = args else {
            return Err(Error::WrongArity);
        };
        let mut dict = dict.write();
        match dict.remove(key) {
            Some(value) => {
                dict.insert(new_key.clone(), value);
                Ok(RedisValue::ok())
            }
            None => Err(Error::NoKey),
        }
    }
}

impl WriteCommand for command::RenameNx {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key, new_key] = args else {
            return Err(Error::WrongArity);
        };
        let mut dict = dict.write();
        if !dict.contains_key(key) {
            return Err(Error::NoKey);
        }
        if dict.contains_key(new_key) {
            return Ok(0.into());
        }
        let value = dict.remove(key).unwrap();
        dict.insert(new_key.clone(), value);
        Ok(1.into())
    }
}

impl ReadCommand for command::Type {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key] = args else {
            return Err(Error::WrongArity);
        };
        let ty = match dict.read().get(key) {
            Some(RedisObject::String(_)) => "string",
            Some(RedisObject::List(_)) => "list",
            None => "none",
        };
        Ok(ty.into())
    }
}
