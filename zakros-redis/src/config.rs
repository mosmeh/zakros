use crate::string::SplitArgsError;
use bstr::ByteSlice;
use serde::{
    de::{self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor},
    Deserialize,
};
use std::{
    collections::{BTreeSet, VecDeque},
    fmt::Display,
    iter::Rev,
    str::FromStr,
};

pub fn from_bytes<'a, T>(input: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    T::deserialize(&mut Deserializer::from_bytes(input))
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    SplitArgs(#[from] SplitArgsError),

    #[error("{0}")]
    ParseNumber(String),

    #[error("argument must be 'yes' or 'no'")]
    InvalidBool,

    #[error("wrong number of arguments")]
    WrongArity,

    #[error("{0}")]
    Custom(String),
}

impl de::Error for ConfigError {
    fn custom<T: Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

type Result<T> = std::result::Result<T, ConfigError>;

struct Deserializer<'de> {
    // lines are processed in reverse order to implement last-wins policy for duplicate keys
    lines: Rev<bstr::Lines<'de>>,

    // records keys for deduplication
    seen_keys: BTreeSet<Vec<u8>>,

    // values in currently processed lines, which are removed as processed
    next_values: VecDeque<Vec<u8>>,
}

impl<'de> Deserializer<'de> {
    fn from_bytes(input: &'de [u8]) -> Self {
        Self {
            lines: input.lines().rev(),
            seen_keys: Default::default(),
            next_values: Default::default(),
        }
    }
}

impl<'de> Deserializer<'de> {
    fn read_next_line(&mut self) -> Result<()> {
        for line in self.lines.by_ref() {
            let line = line.trim_with(|c| matches!(c, ' ' | '\t' | '\r' | '\n'));
            if let Some(b'#') | None = line.first() {
                // comment or blank line
                continue;
            }
            let mut values = crate::string::split_args(line)?;
            let Some(name) = values.first_mut() else {
                continue;
            };
            name.make_ascii_lowercase();
            if self.seen_keys.insert(name.clone()) {
                self.next_values = values.into();
                break;
            }
        }
        Ok(())
    }

    fn peek(&mut self) -> Result<Option<&[u8]>> {
        if self.next_values.is_empty() {
            self.read_next_line()?;
        }
        Ok(self.next_values.front().map(|value| value.as_slice()))
    }

    fn peek_in_current_line(&self) -> Option<&[u8]> {
        self.next_values.front().map(|value| value.as_slice())
    }

    fn consume(&mut self) -> Result<Vec<u8>> {
        if self.next_values.is_empty() {
            self.read_next_line()?;
        }
        match self.next_values.pop_front() {
            Some(value) => Ok(value),
            None => Err(ConfigError::WrongArity),
        }
    }

    fn consume_and_parse_number<T>(&mut self) -> Result<T>
    where
        T: FromStr,
        T::Err: Display,
    {
        self.consume()?
            .to_str()
            .map_err(|err| ConfigError::ParseNumber(err.to_string()))?
            .parse::<T>()
            .map_err(|err| ConfigError::ParseNumber(err.to_string()))
    }

    fn consume_string(&mut self) -> Result<String> {
        let s = self
            .consume()?
            .to_str()
            .map_err(|err| ConfigError::ParseNumber(err.to_string()))?
            .to_owned();
        Ok(s)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = ConfigError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.consume()?;
        if value.eq_ignore_ascii_case(b"yes") {
            return visitor.visit_bool(true);
        }
        if value.eq_ignore_ascii_case(b"no") {
            return visitor.visit_bool(false);
        }
        Err(ConfigError::InvalidBool)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.consume_and_parse_number()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.consume_and_parse_number()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.consume_and_parse_number()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.consume_and_parse_number()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.consume_and_parse_number()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.consume_and_parse_number()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.consume_and_parse_number()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.consume_and_parse_number()?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(self.consume_and_parse_number()?)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(self.consume_and_parse_number()?)
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.consume_string()?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_byte_buf(self.consume()?)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.peek()? {
            Some([]) => {
                self.consume()?;
                visitor.visit_none()
            }
            Some(_) => visitor.visit_some(self),
            None => Err(ConfigError::WrongArity),
        }
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(Seq::new(self)?)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(Map::new(self))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let deserializer = self
            .consume_string()?
            .to_ascii_lowercase()
            .into_deserializer();
        visitor.visit_enum(deserializer)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

struct Seq<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
}

impl<'a, 'de> Seq<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Result<Self> {
        // The end of a line signals the end of this Seq.
        // To detect the end of the line in next_element_seed(), make sure we
        // load values in the next line into next_values here.
        de.peek()?;
        Ok(Self { de })
    }
}

impl<'de, 'a> SeqAccess<'de> for Seq<'a, 'de> {
    type Error = ConfigError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.de.peek_in_current_line().is_some() {
            seed.deserialize(&mut *self.de).map(Some)
        } else {
            Ok(None)
        }
    }
}

struct Map<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
}

impl<'a, 'de> Map<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        Self { de }
    }
}

impl<'de, 'a> MapAccess<'de> for Map<'a, 'de> {
    type Error = ConfigError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        // Assume this Map is the top-level list of config items.
        // The end of a file signals the end of this Map.
        // TODO: support or reject nested maps
        if self.de.peek()?.is_some() {
            seed.deserialize(self.de.consume_string()?.into_deserializer())
                .map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    #[test]
    fn smoke() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(deny_unknown_fields, rename_all = "kebab-case")]
        struct Test {
            number: u32,
            string: String,
            boolean_true: bool,
            boolean_false: bool,
            seq: Vec<String>,
            tuple: (i8, u16),
            empty_optional_string: Option<String>,
            non_empty_optional_string: Option<String>,
        }
        let s = br#"
    # this is a comment

  number 42
STRING foo
boolean-true yes
boolean-false NO
seq  'a'   b "\x52"
tuple -5 8
empty-optional-string ""
non-empty-optional-string "bar"
"#;
        let expected = Test {
            number: 42,
            string: "foo".to_owned(),
            boolean_true: true,
            boolean_false: false,
            seq: vec!["a".to_owned(), "b".to_owned(), "\x52".to_owned()],
            tuple: (-5, 8),
            empty_optional_string: None,
            non_empty_optional_string: Some("bar".to_owned()),
        };
        assert_eq!(expected, super::from_bytes(s).unwrap());
    }
}
