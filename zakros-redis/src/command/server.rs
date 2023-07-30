use super::{Arity, CommandSpec, ReadCommandHandler, StatelessCommandHandler, WriteCommandHandler};
use crate::{
    command,
    error::ResponseError,
    lockable::{ReadLockable, RwLockable},
    resp::Value,
    Dictionary, RedisResult,
};
use std::time::{Duration, SystemTime};

impl CommandSpec for command::DbSize {
    const NAME: &'static str = "DBSIZE";
    const ARITY: Arity = Arity::Fixed(0);
}

impl ReadCommandHandler for command::DbSize {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            Ok((dict.read().len() as i64).into())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }
}

impl CommandSpec for command::Echo {
    const NAME: &'static str = "ECHO";
    const ARITY: Arity = Arity::Fixed(1);
}

impl StatelessCommandHandler for command::Echo {
    fn call(args: &[Vec<u8>]) -> RedisResult {
        match args {
            [message] => Ok(message.clone().into()),
            _ => Err(ResponseError::WrongArity.into()),
        }
    }
}

impl CommandSpec for command::FlushAll {
    const NAME: &'static str = "FLUSHALL";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl WriteCommandHandler for command::FlushAll {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        flush(dict, args)
    }
}

impl CommandSpec for command::FlushDb {
    const NAME: &'static str = "FLUSHDB";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl WriteCommandHandler for command::FlushDb {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        flush(dict, args)
    }
}

impl CommandSpec for command::Info {
    const NAME: &'static str = "INFO";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl CommandSpec for command::Ping {
    const NAME: &'static str = "PING";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl StatelessCommandHandler for command::Ping {
    fn call(args: &[Vec<u8>]) -> RedisResult {
        match args {
            [] => Ok("PONG".into()),
            [message] => Ok(message.clone().into()),
            _ => Err(ResponseError::WrongArity.into()),
        }
    }
}

impl CommandSpec for command::Time {
    const NAME: &'static str = "TIME";
    const ARITY: Arity = Arity::Fixed(0);
}

impl StatelessCommandHandler for command::Time {
    fn call(args: &[Vec<u8>]) -> RedisResult {
        if !args.is_empty() {
            return Err(ResponseError::WrongArity.into());
        }
        let since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Ok(Value::Array(vec![
            Ok(since_epoch.as_secs().to_string().into_bytes().into()),
            Ok(since_epoch.subsec_micros().to_string().into_bytes().into()),
        ]))
    }
}

fn flush<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
    if args.len() <= 1 {
        dict.write().clear();
        Ok(Value::ok())
    } else {
        Err(ResponseError::WrongArity.into())
    }
}
