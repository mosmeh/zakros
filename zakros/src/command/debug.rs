use crate::connection::RedisConnection;
use bstr::ByteSlice;
use bytes::Bytes;
use std::{collections::hash_map::Entry, io::Write};
use zakros_redis::{lockable::RwLockable, resp::Value, BytesExt, RedisResult, ResponseError};

pub fn debug(conn: &RedisConnection, args: &[Bytes]) -> RedisResult {
    let [subcommand, args @ ..] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    match subcommand.to_ascii_uppercase().as_slice() {
        b"LOG" => match args {
            [message] => {
                tracing::warn!("DEBUG LOG: {}", message.as_bstr());
                Ok(Value::ok())
            }
            _ => Err(ResponseError::WrongArity.into()),
        },
        b"POPULATE" => {
            let (count, options) = match args {
                [count, options @ ..] if options.len() < 3 => (count, options),
                _ => return Err(ResponseError::WrongArity.into()),
            };
            let count = count.to_u64()?;
            let size = match options.get(1) {
                Some(size) => Some(size.to_u64()? as usize),
                None => None,
            };
            let prefix = match options.get(0) {
                Some(prefix) => prefix.as_ref(),
                None => b"key",
            };

            // Redis doesn't propagate DEBUG POPULATE to replicas.
            // Likewise, we don't make it go through Raft.
            let mut dict = conn.shared.store.write();
            for i in 0..count {
                let mut key = prefix.to_vec();
                write!(&mut key, ":{}", i).unwrap();

                let entry = dict.entry(key.into());
                let Entry::Vacant(entry) = entry else {
                    continue;
                };

                let mut value = format!("value:{}", i).into_bytes();
                if let Some(size) = size {
                    value.resize(size, 0);
                }
                entry.insert(value.into());
            }

            Ok(Value::ok())
        }
        _ => Err(ResponseError::UnknownSubcommand.into()),
    }
}
