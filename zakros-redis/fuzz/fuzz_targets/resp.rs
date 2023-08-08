#![no_main]

use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut codec = zakros_redis::resp::RespCodec::new();
    let mut bytes = bytes::BytesMut::from(data);
    while let Ok(Some(_)) = codec.decode(&mut bytes) {}
});
