use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(transparent)]
pub struct ByteArray(#[serde(with = "serde_bytes")] pub Vec<u8>);

impl Deref for ByteArray {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ByteArray {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Null,
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Str(String),
    List(Vec<Value>),
    Dict(HashMap<String, Value>),
    Bytes(ByteArray),
}

// Match the deserializer's actual value kind. Untagged enum trial decoding can
// coerce valid-UTF-8 MessagePack bytes into strings and formats large errors for
// every unsuccessful variant; neither behavior belongs at the value boundary.
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_value(deserializer, false)
    }
}

/// A language adapter can preserve unsigned wire integers without changing Rust's default Value convention.
pub struct UnsignedValue(pub Value);

impl<'de> Deserialize<'de> for UnsignedValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_value(deserializer, true).map(Self)
    }
}

struct ValueSeed {
    preserve_unsigned: bool,
}

impl<'de> serde::de::DeserializeSeed<'de> for ValueSeed {
    type Value = Value;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserialize_value(deserializer, self.preserve_unsigned)
    }
}

fn deserialize_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
    preserve_unsigned: bool,
) -> Result<Value, D::Error> {
    struct ValueVisitor {
        preserve_unsigned: bool,
    }

    impl<'de> serde::de::Visitor<'de> for ValueVisitor {
        type Value = Value;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a WAMP value")
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
            Ok(Value::Null)
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> {
            Ok(Value::Null)
        }

        fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Value, E> {
            Ok(Value::Bool(value))
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Value, E> {
            Ok(Value::Int(value))
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Value, E> {
            if self.preserve_unsigned {
                Ok(Value::UInt(value))
            } else {
                Ok(i64::try_from(value).map(Value::Int).unwrap_or(Value::UInt(value)))
            }
        }

        fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Value, E> {
            Ok(Value::Float(value))
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Value, E> {
            Ok(Value::Str(value.into()))
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Value, E> {
            Ok(Value::Str(value))
        }

        fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Value, E> {
            Ok(Value::Bytes(ByteArray(value.into())))
        }

        fn visit_byte_buf<E: serde::de::Error>(self, value: Vec<u8>) -> Result<Value, E> {
            Ok(Value::Bytes(ByteArray(value)))
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(4096));

            while let Some(value) = sequence.next_element_seed(ValueSeed {
                preserve_unsigned: self.preserve_unsigned,
            })? {
                values.push(value);
            }

            Ok(Value::List(values))
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
            let mut values = HashMap::with_capacity(map.size_hint().unwrap_or(0).min(4096));

            while let Some(key) = map.next_key::<String>()? {
                let value = map.next_value_seed(ValueSeed {
                    preserve_unsigned: self.preserve_unsigned,
                })?;
                values.insert(key, value);
            }

            Ok(Value::Dict(values))
        }
    }

    deserializer.deserialize_any(ValueVisitor { preserve_unsigned })
}

impl Value {
    pub fn str<S: Into<String>>(s: S) -> Self {
        Value::Str(s.into())
    }

    pub fn int<N: Into<i64>>(n: N) -> Self {
        Value::Int(n.into())
    }

    pub fn float<N: Into<f64>>(n: N) -> Self {
        Value::Float(n.into())
    }

    pub fn bool(b: bool) -> Self {
        Value::Bool(b)
    }

    pub fn list<A: Into<Vec<Value>>>(a: A) -> Self {
        Value::List(a.into())
    }

    pub fn dict(o: HashMap<String, Value>) -> Self {
        Value::Dict(o)
    }
}

// Implement From for types
impl From<i64> for Value {
    fn from(val: i64) -> Self {
        Value::Int(val)
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Self {
        Value::Float(val)
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self {
        Value::Bool(val)
    }
}

impl From<&str> for Value {
    fn from(val: &str) -> Self {
        Value::Str(val.to_string())
    }
}

impl From<String> for Value {
    fn from(val: String) -> Self {
        Value::Str(val)
    }
}

impl From<&[u8]> for Value {
    fn from(val: &[u8]) -> Self {
        Value::Bytes(ByteArray(val.to_vec()))
    }
}

impl<const N: usize> From<[u8; N]> for Value {
    fn from(val: [u8; N]) -> Self {
        Value::Bytes(ByteArray(val.to_vec()))
    }
}

impl From<Vec<u8>> for Value {
    fn from(val: Vec<u8>) -> Self {
        Value::Bytes(ByteArray(val.to_vec()))
    }
}

impl From<ByteArray> for Value {
    fn from(val: ByteArray) -> Self {
        Value::Bytes(val)
    }
}

#[derive(Debug)]
pub struct Error {
    pub message: String,
}

impl Error {
    pub fn new<T: Into<String>>(msg: T) -> Self {
        Error { message: msg.into() }
    }
}

// Implement Display so errors print nicely
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MyError: {}", self.message)
    }
}

// Implement the std::error::Error trait
impl std::error::Error for Error {}
