use bytes::Bytes;
use zakros_redis::{resp::Value, BytesExt, RedisResult, ResponseError};

pub fn select(args: &[Bytes]) -> RedisResult {
    let [index] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    if index.to_i32()? == 0 {
        Ok(Value::ok())
    } else {
        // TODO: support multiple databases when Raft is disabled
        Err(ResponseError::Other("SELECT is not allowed in cluster mode").into())
    }
}

pub fn shutdown(_: &[Bytes]) -> RedisResult {
    // TODO: gracefully shutdown
    std::process::exit(0)
}
