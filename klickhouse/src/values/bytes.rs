use std::ops::{Deref, DerefMut};

use crate::{unexpected_type, FromSql, Result, ToSql, Type, Value};

/// Wrapper over Vec<u8> to allow more efficient serialization/deserialization of raw bytes
/// The corresponding Clickhouse type here is String or FixedString, not Array(UInt8).
/// Conversion to Array(UInt8) will happen, but it is not efficient.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Bytes(pub Vec<u8>);

impl ToSql for Bytes {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        match type_hint {
            Some(Type::Array(x)) if **x == Type::UInt8 || **x == Type::Int8 => Ok(Value::Array(
                self.0
                    .into_iter()
                    .map(|byte| match &**x {
                        Type::UInt8 => Value::UInt8(byte),
                        Type::Int8 => Value::Int8(byte as i8),
                        _ => unreachable!(),
                    })
                    .collect(),
            )),
            _ => Ok(Value::String(self.0)),
        }
    }
}

impl FromSql for Bytes {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        match type_ {
            Type::String | Type::FixedString(_) => match value {
                Value::String(s) => Ok(Self(s)),
                _ => unreachable!(),
            },
            Type::Array(x) if **x == Type::UInt8 || **x == Type::Int8 => match value {
                Value::Array(values) => Ok(Self(
                    values
                        .into_iter()
                        .map(|x| {
                            Ok(match x {
                                Value::UInt8(x) => x,
                                Value::Int8(x) => x as u8,
                                _ => return Err(unexpected_type(&x.guess_type())),
                            })
                        })
                        .collect::<Result<Vec<u8>>>()?,
                )),
                _ => unreachable!(),
            },
            _ => Err(unexpected_type(type_)),
        }
    }
}

impl Deref for Bytes {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(value: Bytes) -> Self {
        value.0
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}
