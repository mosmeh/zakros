use bytes::Bytes;
use zakros_redis::{error::ResponseError, resp::Value, BytesExt, RedisResult};

pub fn select(args: &[Bytes]) -> RedisResult {
    let [index] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    if index.to_i32()? == 0 {
        Ok(Value::ok())
    } else {
        Err(ResponseError::Other("SELECT is not allowed in cluster mode").into())
    }
}

pub fn shutdown(_: &[Bytes]) -> RedisResult {
    // TODO: gracefully shutdown
    std::process::exit(0)
}
