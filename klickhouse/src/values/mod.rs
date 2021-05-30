use std::borrow::Cow;

use chrono_tz::Tz;

use crate::{
    convert::{unexpected_type, FromSql, ToSql},
    types::Type,
};
use anyhow::*;

mod clickhouse_uuid;
mod date;
mod fixed_point;
mod int256;
mod ip;

pub use date::*;
pub use fixed_point::*;
pub use int256::*;
pub use ip::*;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    Int256(i256),

    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    UInt256(u256),

    Float32(u32),
    Float64(u64),

    Decimal32(usize, i32),
    Decimal64(usize, i64),
    Decimal128(usize, i128),
    Decimal256(usize, i256),

    String(String),

    Uuid(::uuid::Uuid),

    Date(Date),
    DateTime(DateTime),
    DateTime64(Tz, usize, u64),

    Enum8(u8),
    Enum16(u16),

    Array(Vec<Value>),

    // Nested(IndexMap<String, Value>),
    Tuple(Vec<Value>),

    Null,

    Map(Vec<Value>, Vec<Value>),

    Ipv4(Ipv4),
    Ipv6(Ipv6),
}

impl Value {
    pub(crate) fn index_value(&self) -> usize {
        match self {
            Value::UInt8(x) => *x as usize,
            Value::UInt16(x) => *x as usize,
            Value::UInt32(x) => *x as usize,
            Value::UInt64(x) => *x as usize,
            _ => unimplemented!(),
        }
    }

    pub(crate) fn unwrap_array(&self) -> &[Value] {
        match self {
            Value::Array(a) => &a[..],
            _ => unimplemented!(),
        }
    }

    pub fn justify_null<'a>(&'a self, type_: &Type) -> Cow<'a, Value> {
        if self == &Value::Null {
            Cow::Owned(type_.default_value())
        } else {
            Cow::Borrowed(self)
        }
    }

    pub fn to_value<T: FromSql>(self, type_: &Type) -> Result<T> {
        T::from_sql(type_, self)
    }

    pub fn from_value<T: ToSql>(value: T) -> Result<Self> {
        value.to_sql()
    }
}
