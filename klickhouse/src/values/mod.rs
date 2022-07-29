use std::borrow::Cow;

use chrono_tz::Tz;

use crate::{
    convert::{unexpected_type, FromSql, ToSql},
    types::Type,
    Result,
};

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

/// A raw Clickhouse value.
/// Types are not strictly/completely preserved (i.e. types `String` and `FixedString` both are value `String`).
/// Use this if you want dynamically typed queries.
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

    pub(crate) fn justify_null<'a>(&'a self, type_: &Type) -> Cow<'a, Value> {
        if self == &Value::Null {
            Cow::Owned(type_.default_value())
        } else {
            Cow::Borrowed(self)
        }
    }

    /// Converts a [`Value`] to a [`T`] type by calling [`T::from_sql`].
    pub fn to_value<T: FromSql>(self, type_: &Type) -> Result<T> {
        T::from_sql(type_, self)
    }

    /// Converts a [`T`] type to a [`Value`] by calling [`T::to_sql`].
    pub fn from_value<T: ToSql>(value: T) -> Result<Self> {
        value.to_sql()
    }

    /// Guesses a [`Type`] from the value, may not correspond to actual column type in Clickhouse
    pub fn guess_type(&self) -> Type {
        match self {
            Value::Int8(_) => Type::Int8,
            Value::Int16(_) => Type::Int16,
            Value::Int32(_) => Type::Int32,
            Value::Int64(_) => Type::Int64,
            Value::Int128(_) => Type::Int128,
            Value::Int256(_) => Type::Int256,
            Value::UInt8(_) => Type::UInt8,
            Value::UInt16(_) => Type::UInt16,
            Value::UInt32(_) => Type::UInt32,
            Value::UInt64(_) => Type::UInt64,
            Value::UInt128(_) => Type::UInt128,
            Value::UInt256(_) => Type::UInt256,
            Value::Float32(_) => Type::Float32,
            Value::Float64(_) => Type::Float64,
            Value::Decimal32(p, _) => Type::Decimal32(*p),
            Value::Decimal64(p, _) => Type::Decimal64(*p),
            Value::Decimal128(p, _) => Type::Decimal128(*p),
            Value::Decimal256(p, _) => Type::Decimal256(*p),
            Value::String(_) => Type::String,
            Value::Uuid(_) => Type::Uuid,
            Value::Date(_) => Type::Date,
            Value::DateTime(time) => Type::DateTime(time.0),
            Value::DateTime64(tz, p, _) => Type::DateTime64(*p, *tz),
            Value::Enum8(_) => unimplemented!(),
            Value::Enum16(_) => unimplemented!(),
            Value::Array(x) => Type::Array(Box::new(
                x.first().map(|x| x.guess_type()).unwrap_or(Type::String),
            )),
            Value::Tuple(values) => Type::Tuple(values.iter().map(|x| x.guess_type()).collect()),
            Value::Null => Type::Nullable(Box::new(Type::String)),
            Value::Map(k, v) => Type::Map(
                Box::new(k.first().map(|x| x.guess_type()).unwrap_or(Type::String)),
                Box::new(v.first().map(|x| x.guess_type()).unwrap_or(Type::String)),
            ),
            Value::Ipv4(_) => Type::Ipv4,
            Value::Ipv6(_) => Type::Ipv6,
        }
    }
}
