use crate::RedisError;

const HLL_P: usize = 14;
const HLL_Q: usize = u64::BITS as usize - HLL_P;

const HLL_REGISTERS: usize = 1 << HLL_P;
const HLL_P_MASK: u64 = HLL_REGISTERS as u64 - 1;
const HLL_BITS: usize = 6;
const HLL_REGISTER_MAX: u8 = (1 << HLL_BITS) - 1;
const HLL_HDR_SIZE: usize = 16;
const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS + 7) / 8;
const HLL_DENSE: u8 = 0;

const HLL_ALPHA_INF: f64 = 0.7213475204444817;

// TODO: support HLL_SPARSE format

// Format
// +------+---+-----+----------+--------------+
// | HYLL | E | N/U | Cardin.  | Registers... |
// +------+---+-----+----------+--------------+
// HYLL: 4 magic bytes
// E: Encoding in u8. Fixed to HLL_DENSE in this implementation.
// N/U: 3 unused bytes
// Cardin.: u64 in little endian caching the result of count().
//          If the most significant bit of the last byte is 1, the cache is invalidated.
// Registers: HLL_REGISTERS unsigned integers with HLL_BITS bits each

pub struct DenseHyperLogLog<T>(T);

impl<T> DenseHyperLogLog<T> {
    pub fn into_bytes(self) -> T {
        self.0
    }
}

impl DenseHyperLogLog<Vec<u8>> {
    pub fn new() -> Self {
        let mut bytes = vec![0; HLL_DENSE_SIZE];
        bytes[..4].copy_from_slice(b"HYLL".as_slice());
        Self(bytes)
    }
}

impl<T: AsRef<[u8]>> DenseHyperLogLog<T> {
    pub fn from_untrusted_bytes(bytes: T) -> Result<Self, RedisError> {
        let s = bytes.as_ref();
        if s.len() != HLL_DENSE_SIZE {
            return Err(RedisError::WrongType);
        }
        let magic = &s[..4];
        let encoding = s[4];
        if magic == b"HYLL" && encoding == HLL_DENSE {
            Ok(Self(bytes))
        } else {
            Err(RedisError::WrongType)
        }
    }

    fn register(&self, index: usize) -> u8 {
        let bytes = self.0.as_ref();
        let byte_index = HLL_HDR_SIZE + index * HLL_BITS / 8;
        let fb = (index * HLL_BITS) as u32 & 7;
        let fb8 = 8 - fb;
        let b0 = bytes[byte_index];
        let b1 = bytes.get(byte_index + 1).copied().unwrap_or(0);
        (b0.checked_shr(fb).unwrap_or(0) | b1.checked_shl(fb8).unwrap_or(0)) & HLL_REGISTER_MAX
    }

    fn cardinality_cache(&self) -> Option<u64> {
        let bytes = self.0.as_ref();
        (bytes[15] & (1 << 7) == 0).then(|| u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> DenseHyperLogLog<T> {
    pub fn add(&mut self, element: &[u8]) -> bool {
        let hash = murmur_hash64a(element, 0xadc83b19);
        let index = (hash & HLL_P_MASK) as usize;
        let hash = (hash >> HLL_P) | (1 << HLL_Q);
        let count = hash.trailing_zeros() as u8 + 1;
        if count > self.register(index) {
            self.set_register(index, count);
            self.invalidate_cardinality_cache();
            true
        } else {
            false
        }
    }

    pub fn count(&mut self) -> u64 {
        if let Some(cardinality) = self.cardinality_cache() {
            return cardinality;
        }
        let mut histogram = Histogram::new();
        for index in 0..HLL_REGISTERS {
            histogram.add(self.register(index));
        }
        let e = histogram.estimate_cardinality();
        self.set_cardinality_cache(e);
        e
    }
}

impl<T: AsMut<[u8]>> DenseHyperLogLog<T> {
    pub fn copy_registers_from_raw(&mut self, raw: &RawHyperLogLog) {
        for (i, count) in raw.0.iter().enumerate() {
            self.set_register(i, *count);
        }
        self.invalidate_cardinality_cache()
    }

    fn set_register(&mut self, index: usize, value: u8) {
        let bytes = self.0.as_mut();
        let byte_index = HLL_HDR_SIZE + index * HLL_BITS / 8;
        let b0 = &mut bytes[byte_index];
        let fb = (index * HLL_BITS) as u32 & 7;
        *b0 &= !(HLL_REGISTER_MAX.checked_shl(fb).unwrap_or(0));
        *b0 |= value.checked_shl(fb).unwrap_or(0);
        if let Some(b1) = bytes.get_mut(byte_index + 1) {
            let fb8 = 8 - fb;
            *b1 &= !(HLL_REGISTER_MAX.checked_shr(fb8).unwrap_or(0));
            *b1 |= value.checked_shr(fb8).unwrap_or(0);
        }
    }

    fn invalidate_cardinality_cache(&mut self) {
        self.0.as_mut()[15] |= 1 << 7;
    }

    fn set_cardinality_cache(&mut self, cardinality: u64) {
        self.0.as_mut()[8..16].copy_from_slice(&cardinality.to_le_bytes());
    }
}

pub struct RawHyperLogLog([u8; HLL_REGISTERS]);

impl RawHyperLogLog {
    pub const fn new() -> Self {
        Self([0; HLL_REGISTERS])
    }

    pub fn count(&self) -> u64 {
        let mut histogram = Histogram::new();
        for count in self.0 {
            histogram.add(count);
        }
        histogram.estimate_cardinality()
    }

    pub fn merge_dense<T: AsRef<[u8]>>(&mut self, dense: &DenseHyperLogLog<T>) {
        for (i, dest) in self.0.iter_mut().enumerate() {
            let count = dense.register(i);
            if count > *dest {
                *dest = count;
            }
        }
    }
}

struct Histogram([u16; 1 << HLL_BITS]);

impl Histogram {
    const fn new() -> Self {
        Self([0; 1 << HLL_BITS])
    }

    fn add(&mut self, count: u8) {
        self.0[count as usize] += 1;
    }

    fn estimate_cardinality(&self) -> u64 {
        const M: f64 = HLL_REGISTERS as f64;
        let mut z = M * tau((M - self.0[HLL_Q + 1] as f64) / M);
        for &freq in self.0[1..=HLL_Q].iter().rev() {
            z += freq as f64;
            z *= 0.5;
        }
        z += M * sigma(self.0[0] as f64 / M);
        (HLL_ALPHA_INF * M * M / z).round() as u64
    }
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= f64::powf(1.0 - x, 2.0) * y;
        if z_prime == z {
            return z / 3.0;
        }
    }
}

const fn murmur_hash64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u8 = 47;

    let len = key.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(M));

    let end = len - (len & 7);
    let mut i = 0;
    while i < end {
        let mut k = u64::from_le_bytes([
            key[i],
            key[i + 1],
            key[i + 2],
            key[i + 3],
            key[i + 4],
            key[i + 5],
            key[i + 6],
            key[i + 7],
        ]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
        i += 8;
    }

    let remainder = len & 7;
    if remainder == 7 {
        h ^= (key[i + 6] as u64) << 48;
    }
    if remainder >= 6 {
        h ^= (key[i + 5] as u64) << 40;
    }
    if remainder >= 5 {
        h ^= (key[i + 4] as u64) << 32;
    }
    if remainder >= 4 {
        h ^= (key[i + 3] as u64) << 24;
    }
    if remainder >= 3 {
        h ^= (key[i + 2] as u64) << 16;
    }
    if remainder >= 2 {
        h ^= (key[i + 1] as u64) << 8;
    }
    if remainder >= 1 {
        h ^= key[i] as u64;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_xoshiro::{
        rand_core::{RngCore, SeedableRng},
        Xoshiro256PlusPlus,
    };

    #[test]
    fn registers() {
        const HLL_TEST_CYCLES: usize = 1000;

        let mut hll = DenseHyperLogLog::new();
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(42);
        for _ in 0..HLL_TEST_CYCLES {
            let mut byte_counters = Vec::with_capacity(HLL_REGISTERS);
            for i in 0..HLL_REGISTERS {
                let r = rng.next_u32() as u8 & HLL_REGISTER_MAX;
                byte_counters.push(r);
                hll.set_register(i, r);
            }
            for (i, byte_counter) in byte_counters.into_iter().enumerate() {
                assert_eq!(hll.register(i), byte_counter);
            }
        }
    }

    #[test]
    fn approximation_error() {
        const SEED: i64 = 42;

        let mut hll = DenseHyperLogLog::new();
        let rel_err: f64 = 1.04 / (HLL_REGISTERS as f64).sqrt();
        let mut checkpoint = 1;
        for i in 1..=10000000 {
            hll.add(&(i ^ SEED).to_le_bytes());
            if i != checkpoint {
                continue;
            }
            let abs_err = (i - hll.count() as i64).unsigned_abs();
            let max_err = if i == 10 {
                1
            } else {
                (rel_err * 6.0 * checkpoint as f64).ceil() as u64
            };
            assert!(abs_err <= max_err);
            checkpoint *= 10;
        }
    }
}
