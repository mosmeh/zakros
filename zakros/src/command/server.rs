use super::{ReadCommandHandler, StatelessCommandHandler, WriteCommandHandler};
use crate::{
    command,
    error::Error,
    resp::RedisValue,
    store::{Dictionary, ReadLockable, RwLockable},
    RedisResult,
};
use std::time::{Duration, SystemTime};

impl ReadCommandHandler for command::DbSize {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            Ok((dict.read().len() as i64).into())
        } else {
            Err(Error::WrongArity)
        }
    }
}

impl StatelessCommandHandler for command::Echo {
    fn call(args: &[Vec<u8>]) -> RedisResult {
        match args {
            [message] => Ok(message.clone().into()),
            _ => Err(Error::WrongArity),
        }
    }
}

impl WriteCommandHandler for command::FlushAll {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        flush(dict, args)
    }
}

impl WriteCommandHandler for command::FlushDb {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
        flush(dict, args)
    }
}

impl StatelessCommandHandler for command::Ping {
    fn call(args: &[Vec<u8>]) -> RedisResult {
        match args {
            [] => Ok("PONG".into()),
            [message] => Ok(message.clone().into()),
            _ => Err(Error::WrongArity),
        }
    }
}

impl StatelessCommandHandler for command::Time {
    fn call(args: &[Vec<u8>]) -> RedisResult {
        if !args.is_empty() {
            return Err(Error::WrongArity);
        }
        let since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Ok(RedisValue::Array(vec![
            since_epoch.as_secs().to_string().into_bytes().into(),
            since_epoch.subsec_micros().to_string().into_bytes().into(),
        ]))
    }
}

fn flush<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Vec<u8>]) -> RedisResult {
    if args.is_empty() {
        dict.write().clear();
        Ok(RedisValue::ok())
    } else {
        Err(Error::WrongArity)
    }
}
