use super::{Arity, CommandSpec, ReadCommandHandler, StatelessCommandHandler, WriteCommandHandler};
use crate::{
    command,
    error::Error,
    object::{BytesExt, RedisObject},
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};

impl CommandSpec for command::Del {
    const NAME: &'static str = "DEL";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::Del {
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

impl CommandSpec for command::Exists {
    const NAME: &'static str = "EXISTS";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::Exists {
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

impl CommandSpec for command::Move {
    const NAME: &'static str = "MOVE";
    const ARITY: Arity = Arity::Fixed(2);
}

impl StatelessCommandHandler for command::Move {
    fn call(_: &[Vec<u8>]) -> RedisResult {
        Err(Error::Custom(
            "ERR MOVE is not allowed in cluster mode".to_owned(),
        ))
    }
}

impl CommandSpec for command::Keys {
    const NAME: &'static str = "KEYS";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::Keys {
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

impl CommandSpec for command::Rename {
    const NAME: &'static str = "RENAME";
    const ARITY: Arity = Arity::Fixed(2);
}

impl WriteCommandHandler for command::Rename {
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

impl CommandSpec for command::RenameNx {
    const NAME: &'static str = "RENAMENX";
    const ARITY: Arity = Arity::Fixed(2);
}

impl WriteCommandHandler for command::RenameNx {
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

impl CommandSpec for command::Select {
    const NAME: &'static str = "SELECT";
    const ARITY: Arity = Arity::Fixed(1);
}

impl StatelessCommandHandler for command::Select {
    fn call(args: &[Vec<u8>]) -> RedisResult {
        let [index] = args else {
            return Err(Error::WrongArity);
        };
        if index.to_i32()? == 0 {
            Ok(RedisValue::ok())
        } else {
            Err(Error::Custom(
                "ERR SELECT is not allowed in cluster mode".to_owned(),
            ))
        }
    }
}

impl CommandSpec for command::Shutdown {
    const NAME: &'static str = "SHUTDOWN";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl StatelessCommandHandler for command::Shutdown {
    fn call(_: &[Vec<u8>]) -> RedisResult {
        // TODO: gracefully shutdown
        std::process::exit(0)
    }
}

impl CommandSpec for command::Type {
    const NAME: &'static str = "TYPE";
    const ARITY: Arity = Arity::Fixed(1);
}

impl ReadCommandHandler for command::Type {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        let [key] = args else {
            return Err(Error::WrongArity);
        };
        let ty = match dict.read().get(key) {
            Some(RedisObject::String(_)) => "string",
            Some(RedisObject::List(_)) => "list",
            Some(RedisObject::Set(_)) => "set",
            Some(RedisObject::Hash(_)) => "hash",
            None => "none",
        };
        Ok(ty.into())
    }
}
