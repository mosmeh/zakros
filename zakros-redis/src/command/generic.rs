use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    lockable::{ReadLockable, RwLockable},
    resp::Value,
    Dictionary, Object, RedisResult, ResponseError,
};
use bytes::Bytes;

impl CommandSpec for command::Del {
    const NAME: &'static str = "DEL";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::Del {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            return Err(ResponseError::WrongArity.into());
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

impl CommandSpec for command::Exists {
    const NAME: &'static str = "EXISTS";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::Exists {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            return Err(ResponseError::WrongArity.into());
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

impl CommandSpec for command::Keys {
    const NAME: &'static str = "KEYS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::Keys {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [pattern] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let keys = dict
            .read()
            .keys()
            .filter(|key| crate::string::string_match(pattern, key))
            .map(|key| Ok(key.clone().into()))
            .collect();
        Ok(Value::Array(keys))
    }
}

impl CommandSpec for command::Rename {
    const NAME: &'static str = "RENAME";
    const ARITY: Arity = Arity::Fixed(2);
}

impl WriteCommandHandler for command::Rename {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, new_key] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut dict = dict.write();
        match dict.remove(key) {
            Some(value) => {
                dict.insert(new_key.clone(), value);
                Ok(Value::ok())
            }
            None => Err(ResponseError::NoKey.into()),
        }
    }
}

impl CommandSpec for command::RenameNx {
    const NAME: &'static str = "RENAMENX";
    const ARITY: Arity = Arity::Fixed(2);
}

impl WriteCommandHandler for command::RenameNx {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, new_key] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut dict = dict.write();
        if !dict.contains_key(key) {
            return Err(ResponseError::NoKey.into());
        }
        if dict.contains_key(new_key) {
            return Ok(0.into());
        }
        let value = dict.remove(key).unwrap();
        dict.insert(new_key.clone(), value);
        Ok(1.into())
    }
}

impl CommandSpec for command::Type {
    const NAME: &'static str = "TYPE";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::Type {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let ty = match dict.read().get(key) {
            Some(Object::String(_)) => "string",
            Some(Object::List(_)) => "list",
            Some(Object::Set(_)) => "set",
            Some(Object::Hash(_)) => "hash",
            None => "none",
        };
        Ok(ty.into())
    }
}

impl CommandSpec for command::Unlink {
    const NAME: &'static str = "UNLINK";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::Unlink {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            return Err(ResponseError::WrongArity.into());
        }
        let mut unlinked = Vec::with_capacity(args.len());
        {
            let mut dict = dict.write();
            for key in args {
                if let Some(kv) = dict.remove_entry(key) {
                    unlinked.push(kv);
                }
            }
        }
        let num_unlinked = unlinked.len();
        tokio::task::spawn_blocking(|| drop(unlinked));
        Ok((num_unlinked as i64).into())
    }
}
