use super::{Arity, CommandSpec, WriteCommandHandler};
use crate::{
    command,
    hyperloglog::{DenseHyperLogLog, RawHyperLogLog},
    lockable::RwLockable,
    resp::Value,
    Dictionary, Object, RedisError, RedisResult, ResponseError,
};
use bytes::Bytes;
use std::collections::hash_map::Entry;

impl CommandSpec for command::PfAdd {
    const NAME: &'static str = "PFADD";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::PfAdd {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, elements @ ..] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match dict.write().entry(key.clone()) {
            Entry::Occupied(entry) => {
                let Object::String(s) = entry.into_mut() else {
                    return Err(RedisError::WrongType);
                };
                let mut hll = DenseHyperLogLog::from_untrusted_bytes(s)?;
                let mut is_updated = false;
                for element in elements {
                    is_updated |= hll.add(element);
                }
                Ok((is_updated as i64).into())
            }
            Entry::Vacant(entry) => {
                let mut hll = DenseHyperLogLog::new();
                for element in elements {
                    hll.add(element);
                }
                entry.insert(hll.into_bytes().into());
                Ok(1.into())
            }
        }
    }
}

impl CommandSpec for command::PfCount {
    const NAME: &'static str = "PFCOUNT";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::PfCount {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        match args {
            [] => Err(ResponseError::WrongArity.into()),
            [key] => {
                let mut dict = dict.write();
                let Some(object) = dict.get_mut(key) else {
                    return Ok(0.into());
                };
                let Object::String(s) = object else {
                    return Err(RedisError::WrongType);
                };
                let mut hll = DenseHyperLogLog::from_untrusted_bytes(s)?;
                Ok((hll.count() as i64).into())
            }
            keys => {
                let dict = dict.read();
                let mut hll = RawHyperLogLog::new();
                for key in keys {
                    let Some(object) = dict.get(key) else {
                        continue;
                    };
                    let Object::String(s) = object else {
                        return Err(RedisError::WrongType);
                    };
                    hll.merge_dense(&DenseHyperLogLog::from_untrusted_bytes(s)?);
                }
                Ok((hll.count() as i64).into())
            }
        }
    }
}

impl CommandSpec for command::PfMerge {
    const NAME: &'static str = "PFMERGE";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl WriteCommandHandler for command::PfMerge {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [dest_key, source_keys @ ..] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let mut hll = RawHyperLogLog::new();
        let mut dict = dict.write();
        if let Some(object) = dict.get(dest_key) {
            let Object::String(s) = object else {
                return Err(RedisError::WrongType);
            };
            hll.merge_dense(&DenseHyperLogLog::from_untrusted_bytes(s)?);
        }
        for key in source_keys {
            if let Some(object) = dict.get(key) {
                let Object::String(s) = object else {
                    return Err(RedisError::WrongType);
                };
                hll.merge_dense(&DenseHyperLogLog::from_untrusted_bytes(s)?);
            }
        }
        match dict.entry(dest_key.clone()) {
            Entry::Occupied(entry) => {
                let Object::String(s) = entry.into_mut() else {
                    unreachable!()
                };
                DenseHyperLogLog::from_untrusted_bytes(s)
                    .unwrap() // already validated above
                    .copy_registers_from_raw(&hll);
            }
            Entry::Vacant(entry) => {
                let mut dest_hll = DenseHyperLogLog::new();
                dest_hll.copy_registers_from_raw(&hll);
                entry.insert(dest_hll.into_bytes().into());
            }
        }
        Ok(Value::ok())
    }
}
