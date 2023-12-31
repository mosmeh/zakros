use super::{Arity, CommandSpec, ReadCommandHandler, WriteCommandHandler};
use crate::{
    command,
    lockable::{ReadLockable, RwLockable},
    BytesExt, Dictionary, Object, RedisError, RedisResult, ResponseError,
};
use bytes::{BufMut, Bytes};
use std::collections::hash_map::Entry;

impl CommandSpec for command::BitCount {
    const NAME: &'static str = "BITCOUNT";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl ReadCommandHandler for command::BitCount {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, options @ ..] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let dict = dict.read();
        let s = match dict.get(key) {
            Some(Object::String(s)) => s,
            Some(_) => return Err(RedisError::WrongType),
            None => return Ok(0.into()),
        };
        if s.is_empty() {
            return Ok(0.into());
        }
        let mut first_byte_neg_mask = 0;
        let mut last_byte_neg_mask = 0;
        let (start, end) = match options {
            [] => (0, s.len() - 1),
            [start, end, options @ ..] => {
                let mut start = start.to_i64()?;
                let mut end = end.to_i64()?;
                if start < 0 && end < 0 && start > end {
                    return Ok(0.into());
                }
                let is_bit = match options {
                    [] => false,
                    [indexing] => match indexing.to_ascii_uppercase().as_slice() {
                        b"BYTE" => false,
                        b"BIT" => true,
                        _ => return Err(ResponseError::SyntaxError.into()),
                    },
                    _ => return Err(ResponseError::SyntaxError.into()),
                };
                let len = if is_bit { s.len() << 3 } else { s.len() } as i64;
                if start < 0 {
                    start = (start + len).max(0);
                }
                if end < 0 {
                    end = (end + len).max(0);
                }
                end = end.min(len - 1);
                if start > end {
                    return Ok(0.into());
                }
                let start = start as usize;
                let end = end as usize;
                if is_bit {
                    let (start, bit_offset) = decompose_offset(start);
                    first_byte_neg_mask = !((1 << (bit_offset + 1)) - 1) as u8;
                    let (end, bit_offset) = decompose_offset(end);
                    last_byte_neg_mask = (1 << bit_offset) - 1;
                    (start, end)
                } else {
                    (start, end)
                }
            }
            _ => return Err(ResponseError::SyntaxError.into()),
        };
        let mut count = 0;
        for byte in &s[start..=end] {
            count += byte.count_ones();
        }
        if first_byte_neg_mask != 0 {
            count -= (s[start] & first_byte_neg_mask).count_ones();
        }
        if last_byte_neg_mask != 0 {
            count -= (s[end] & last_byte_neg_mask).count_ones();
        }
        Ok((count as i64).into())
    }
}

impl CommandSpec for command::BitOp {
    const NAME: &'static str = "BITOP";
    const ARITY: Arity = Arity::AtLeast(3);
}

impl WriteCommandHandler for command::BitOp {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let (op, dest_key, keys) = match args {
            [op, dest_key, keys @ ..] if !keys.is_empty() => (op, dest_key, keys),
            _ => return Err(ResponseError::WrongArity.into()),
        };
        let op = match op.to_ascii_uppercase().as_slice() {
            b"AND" => BitOp::And,
            b"OR" => BitOp::Or,
            b"XOR" => BitOp::Xor,
            b"NOT" => BitOp::Not,
            _ => return Err(ResponseError::SyntaxError.into()),
        };
        if op == BitOp::Not && keys.len() != 1 {
            return Err(RedisError::Response(ResponseError::Other(
                "BITOP NOT must be called with a single source key.",
            )));
        }
        let mut sources = Vec::with_capacity(keys.len());
        let mut max_len = 0;
        let mut dict = dict.write();
        for key in keys {
            match dict.get(key) {
                Some(Object::String(s)) => {
                    sources.push(s.iter().fuse());
                    max_len = max_len.max(s.len());
                }
                Some(_) => return Err(RedisError::WrongType),
                None => (),
            }
        }
        if max_len == 0 {
            dict.remove(dest_key);
            return Ok(0.into());
        }
        if op == BitOp::And && sources.len() < keys.len() {
            dict.insert(dest_key.clone(), vec![0; max_len].into());
            return Ok((max_len as i64).into());
        }
        let Some((first, rest)) = sources.split_first_mut() else {
            dict.remove(dest_key);
            return Ok(0.into());
        };
        let mut dest_bytes = Vec::with_capacity(max_len);
        for _ in 0..max_len {
            let mut dest_byte = first.next().copied().unwrap_or(0);
            if op == BitOp::Not {
                dest_bytes.put_u8(!dest_byte);
                continue;
            }
            for iter in &mut *rest {
                let source_byte = iter.next().copied().unwrap_or(0);
                match op {
                    BitOp::And => dest_byte &= source_byte,
                    BitOp::Or => dest_byte |= source_byte,
                    BitOp::Xor => dest_byte ^= source_byte,
                    BitOp::Not => unreachable!(),
                }
            }
            dest_bytes.put_u8(dest_byte);
        }
        dict.insert(dest_key.clone(), dest_bytes.into());
        Ok((max_len as i64).into())
    }
}

impl CommandSpec for command::GetBit {
    const NAME: &'static str = "GETBIT";
    const ARITY: Arity = Arity::Fixed(2);
}

impl ReadCommandHandler for command::GetBit {
    fn call<'a, D: ReadLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, offset] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let offset = offset.to_u64()?;
        let dict = dict.read();
        let s = match dict.get(key) {
            Some(Object::String(s)) => s,
            Some(_) => return Err(RedisError::WrongType),
            None => return Ok(0.into()),
        };
        let (byte_index, bit_offset) = decompose_offset(offset as usize);
        let value = match s.get(byte_index) {
            Some(byte) => (byte & (1 << bit_offset) > 0) as i64,
            None => 0,
        };
        Ok(value.into())
    }
}

impl CommandSpec for command::SetBit {
    const NAME: &'static str = "SETBIT";
    const ARITY: Arity = Arity::Fixed(3);
}

impl WriteCommandHandler for command::SetBit {
    fn call<'a, D: RwLockable<'a, Dictionary>>(dict: &'a D, args: &[Bytes]) -> RedisResult {
        let [key, offset, value] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let offset = offset.to_u64()?;
        let value = value.to_i64()?;
        if value != 0 && value != 1 {
            return Err(ResponseError::ValueOutOfRange.into());
        }
        let value = value as u8;
        let (byte_index, bit_offset) = decompose_offset(offset as usize);
        let required_len = byte_index + 1;
        match dict.write().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let Object::String(s) = entry.get_mut() else {
                    return Err(RedisError::WrongType);
                };
                if s.len() < required_len {
                    s.resize(required_len, 0);
                }
                let byte = &mut s[byte_index];
                let original_value = (*byte & (1 << bit_offset) > 0) as i64;
                *byte &= !(1 << bit_offset);
                *byte |= value << bit_offset;
                Ok(original_value.into())
            }
            Entry::Vacant(entry) => {
                let mut bytes = vec![0; required_len];
                bytes[byte_index] |= value << bit_offset;
                entry.insert(bytes.into());
                Ok(0.into())
            }
        }
    }
}

const fn decompose_offset(offset: usize) -> (usize, u8) {
    (offset >> 3, 7 - (offset as u8 & 0x7))
}

#[derive(PartialEq)]
enum BitOp {
    And,
    Or,
    Xor,
    Not,
}
