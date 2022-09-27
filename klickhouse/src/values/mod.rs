use std::{borrow::Cow, fmt};

use chrono::{SecondsFormat, Utc};
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

    Enum8(i8),
    Enum16(i16),

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

fn escape_string(f: &mut fmt::Formatter<'_>, from: &str) -> fmt::Result {
    for (i, c) in from.char_indices() {
        let c_int = c as u32;
        if c_int < 128 {
            match c_int as u8 {
                b'\\' => write!(f, "\\\\")?,
                b'\'' => write!(f, "\\'")?,
                0x08 => write!(f, "\\b")?,
                0x0C => write!(f, "\\f")?,
                b'\r' => write!(f, "\\r")?,
                b'\n' => write!(f, "\\n")?,
                b'\t' => write!(f, "\\t")?,
                b'\0' => write!(f, "\\0")?,
                0x07 => write!(f, "\\a")?,
                0x0B => write!(f, "\\v")?,
                _ => write!(f, "{c}")?,
            }
        } else {
            for i in i..i + c.len_utf8() {
                let byte = from.as_bytes()[i];
                write!(f, "\\x{byte:02X}")?;
            }
        }
    }
    Ok(())
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int8(x) => write!(f, "{x}"),
            Value::Int16(x) => write!(f, "{x}"),
            Value::Int32(x) => write!(f, "{x}"),
            Value::Int64(x) => write!(f, "{x}"),
            Value::Int128(x) => write!(f, "{x}"),
            Value::Int256(x) => write!(f, "{x}"),
            Value::UInt8(x) => write!(f, "{x}"),
            Value::UInt16(x) => write!(f, "{x}"),
            Value::UInt32(x) => write!(f, "{x}"),
            Value::UInt64(x) => write!(f, "{x}"),
            Value::UInt128(x) => write!(f, "{x}"),
            Value::UInt256(x) => write!(f, "{x}"),
            Value::Float32(x) => write!(f, "{x}"),
            Value::Float64(x) => write!(f, "{x}"),
            Value::Decimal32(precision, value) => {
                let raw_value = value.to_string();
                if raw_value.len() < *precision {
                    write!(f, "{raw_value}")
                } else {
                    let pre = &raw_value[..raw_value.len() - precision];
                    let fraction = &raw_value[raw_value.len() - precision..];
                    write!(f, "{pre}.{fraction}")
                }
            }
            Value::Decimal64(precision, value) => {
                let raw_value = value.to_string();
                if raw_value.len() < *precision {
                    write!(f, "{raw_value}")
                } else {
                    let pre = &raw_value[..raw_value.len() - precision];
                    let fraction = &raw_value[raw_value.len() - precision..];
                    write!(f, "{pre}.{fraction}")
                }
            }
            Value::Decimal128(precision, value) => {
                let raw_value = value.to_string();
                if raw_value.len() < *precision {
                    write!(f, "{raw_value}")
                } else {
                    let pre = &raw_value[..raw_value.len() - precision];
                    let fraction = &raw_value[raw_value.len() - precision..];
                    write!(f, "{pre}.{fraction}")
                }
            }
            Value::Decimal256(..) => {
                unimplemented!("Decimal256 display not implemented");
            }
            Value::String(string) => {
                write!(f, "'")?;
                escape_string(f, &**string)?;
                write!(f, "'")
            }
            Value::Uuid(uuid) => {
                write!(f, "'{}'", uuid)
            }
            Value::Date(date) => {
                let chrono_date: chrono::Date<Utc> = (*date).into();
                write!(f, "'{}'", chrono_date.format("%Y-%m-%d"))
            }
            Value::DateTime(datetime) => {
                let chrono_date: chrono::DateTime<Tz> =
                    (*datetime).try_into().map_err(|_| fmt::Error)?;
                let string = chrono_date.to_rfc3339_opts(SecondsFormat::AutoSi, true);
                write!(f, "'")?;
                escape_string(f, &*string)?;
                write!(f, "'")
            }
            Value::DateTime64(tz, precision, _) => {
                let chrono_date: chrono::DateTime<Tz> =
                    FromSql::from_sql(&Type::DateTime64(*precision, *tz), self.clone())
                        .map_err(|_| fmt::Error)?;
                let string = chrono_date.to_rfc3339_opts(SecondsFormat::AutoSi, true);
                write!(f, "parseDateTime64BestEffort('")?;
                escape_string(f, &*string)?;
                write!(f, "', {precision})")
            }
            Value::Enum8(x) => write!(f, "{x}"),
            Value::Enum16(x) => write!(f, "{x}"),
            Value::Array(array) => {
                write!(f, "[")?;
                if let Some(item) = array.get(0) {
                    write!(f, "{}", item)?;
                }
                for item in array.iter().skip(1) {
                    write!(f, ",{}", item)?;
                }
                write!(f, "]")
            }
            Value::Tuple(tuple) => {
                write!(f, "(")?;
                if let Some(item) = tuple.get(0) {
                    write!(f, "{}", item)?;
                }
                for item in tuple.iter().skip(1) {
                    write!(f, ",{}", item)?;
                }
                write!(f, ")")
            }
            Value::Null => write!(f, "NULL"),
            Value::Map(keys, values) => {
                assert_eq!(keys.len(), values.len());
                write!(f, "{{")?;
                let mut iter = keys.iter().zip(values.iter());
                if let Some((key, value)) = iter.next() {
                    write!(f, "{key}:{value}")?;
                }
                for (key, value) in iter {
                    write!(f, ",{key}:{value}")?;
                }
                write!(f, "}}")
            }
            Value::Ipv4(ipv4) => write!(f, "'{ipv4}'"),
            Value::Ipv6(ipv6) => write!(f, "'{ipv6}'"),
        }
    }
}
