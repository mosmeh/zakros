#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: (&[u8], &[u8])| {
    let (pattern, string) = data;
    zakros_redis::string::string_match(pattern, string);
});
