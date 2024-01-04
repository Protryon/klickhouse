use std::{borrow::Cow, fmt, hash::Hash};

use chrono::{NaiveDate, SecondsFormat};
use chrono_tz::Tz;

use crate::{
    convert::{unexpected_type, FromSql, ToSql},
    types::Type,
    Result,
};

mod bytes;
mod clickhouse_uuid;
mod date;
#[cfg(feature = "rust_decimal")]
mod decimal;
mod fixed_point;
mod geo;
mod int256;
mod ip;

pub use bytes::*;
pub use date::*;
pub use fixed_point::*;
pub use geo::*;
pub use int256::*;
pub use ip::*;

#[cfg(test)]
mod tests;

/// A raw Clickhouse value.
/// Types are not strictly/completely preserved (i.e. types `String` and `FixedString` both are value `String`).
/// Use this if you want dynamically typed queries.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    Float32(f32),
    Float64(f64),

    Decimal32(usize, i32),
    Decimal64(usize, i64),
    Decimal128(usize, i128),
    Decimal256(usize, i256),

    String(Vec<u8>),

    Uuid(::uuid::Uuid),

    Date(Date),
    DateTime(DateTime),
    DateTime64(DynDateTime64),

    Enum8(i8),
    Enum16(i16),

    Array(Vec<Value>),

    Tuple(Vec<Value>),

    Null,

    Map(Vec<Value>, Vec<Value>),

    Ipv4(Ipv4),
    Ipv6(Ipv6),

    Point(Point),
    Ring(Ring),
    Polygon(Polygon),
    MultiPolygon(MultiPolygon),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int8(l0), Self::Int8(r0)) => l0 == r0,
            (Self::Int16(l0), Self::Int16(r0)) => l0 == r0,
            (Self::Int32(l0), Self::Int32(r0)) => l0 == r0,
            (Self::Int64(l0), Self::Int64(r0)) => l0 == r0,
            (Self::Int128(l0), Self::Int128(r0)) => l0 == r0,
            (Self::Int256(l0), Self::Int256(r0)) => l0 == r0,
            (Self::UInt8(l0), Self::UInt8(r0)) => l0 == r0,
            (Self::UInt16(l0), Self::UInt16(r0)) => l0 == r0,
            (Self::UInt32(l0), Self::UInt32(r0)) => l0 == r0,
            (Self::UInt64(l0), Self::UInt64(r0)) => l0 == r0,
            (Self::UInt128(l0), Self::UInt128(r0)) => l0 == r0,
            (Self::UInt256(l0), Self::UInt256(r0)) => l0 == r0,
            (Self::Float32(l0), Self::Float32(r0)) => l0.to_bits() == r0.to_bits(),
            (Self::Float64(l0), Self::Float64(r0)) => l0.to_bits() == r0.to_bits(),
            (Self::Decimal32(l0, l1), Self::Decimal32(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Decimal64(l0, l1), Self::Decimal64(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Decimal128(l0, l1), Self::Decimal128(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Decimal256(l0, l1), Self::Decimal256(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Uuid(l0), Self::Uuid(r0)) => l0 == r0,
            (Self::Date(l0), Self::Date(r0)) => l0 == r0,
            (Self::DateTime(l0), Self::DateTime(r0)) => l0 == r0,
            (Self::DateTime64(l0), Self::DateTime64(r0)) => l0 == r0,
            (Self::Enum8(l0), Self::Enum8(r0)) => l0 == r0,
            (Self::Enum16(l0), Self::Enum16(r0)) => l0 == r0,
            (Self::Array(l0), Self::Array(r0)) => l0 == r0,
            (Self::Tuple(l0), Self::Tuple(r0)) => l0 == r0,
            (Self::Map(l0, l1), Self::Map(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Ipv4(l0), Self::Ipv4(r0)) => l0 == r0,
            (Self::Ipv6(l0), Self::Ipv6(r0)) => l0 == r0,
            (Self::Point(l0), Self::Point(r0)) => l0 == r0,
            (Self::Ring(l0), Self::Ring(r0)) => l0 == r0,
            (Self::Polygon(l0), Self::Polygon(r0)) => l0 == r0,
            (Self::MultiPolygon(l0), Self::MultiPolygon(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl Hash for Value {
    fn hash<H: ::core::hash::Hasher>(&self, state: &mut H) {
        Hash::hash(&core::mem::discriminant(self), state);
        match self {
            Value::Int8(x) => ::core::hash::Hash::hash(x, state),
            Value::Int16(x) => ::core::hash::Hash::hash(x, state),
            Value::Int32(x) => ::core::hash::Hash::hash(x, state),
            Value::Int64(x) => ::core::hash::Hash::hash(x, state),
            Value::Int128(x) => ::core::hash::Hash::hash(x, state),
            Value::Int256(x) => ::core::hash::Hash::hash(x, state),
            Value::UInt8(x) => ::core::hash::Hash::hash(x, state),
            Value::UInt16(x) => ::core::hash::Hash::hash(x, state),
            Value::UInt32(x) => ::core::hash::Hash::hash(x, state),
            Value::UInt64(x) => ::core::hash::Hash::hash(x, state),
            Value::UInt128(x) => ::core::hash::Hash::hash(x, state),
            Value::UInt256(x) => ::core::hash::Hash::hash(x, state),
            Value::Float32(x) => ::core::hash::Hash::hash(&x.to_bits(), state),
            Value::Float64(x) => ::core::hash::Hash::hash(&x.to_bits(), state),
            Value::Decimal32(x, __self_1) => {
                ::core::hash::Hash::hash(x, state);
                ::core::hash::Hash::hash(__self_1, state)
            }
            Value::Decimal64(x, __self_1) => {
                ::core::hash::Hash::hash(x, state);
                ::core::hash::Hash::hash(__self_1, state)
            }
            Value::Decimal128(x, __self_1) => {
                ::core::hash::Hash::hash(x, state);
                ::core::hash::Hash::hash(__self_1, state)
            }
            Value::Decimal256(x, __self_1) => {
                ::core::hash::Hash::hash(x, state);
                ::core::hash::Hash::hash(__self_1, state)
            }
            Value::String(x) => ::core::hash::Hash::hash(x, state),
            Value::Uuid(x) => ::core::hash::Hash::hash(x, state),
            Value::Date(x) => ::core::hash::Hash::hash(x, state),
            Value::DateTime(x) => ::core::hash::Hash::hash(x, state),
            Value::DateTime64(x) => {
                ::core::hash::Hash::hash(x, state);
            }
            Value::Enum8(x) => ::core::hash::Hash::hash(x, state),
            Value::Enum16(x) => ::core::hash::Hash::hash(x, state),
            Value::Array(x) => ::core::hash::Hash::hash(x, state),
            Value::Tuple(x) => ::core::hash::Hash::hash(x, state),
            Value::Map(x, __self_1) => {
                ::core::hash::Hash::hash(x, state);
                ::core::hash::Hash::hash(__self_1, state)
            }
            Value::Ipv4(x) => ::core::hash::Hash::hash(x, state),
            Value::Ipv6(x) => ::core::hash::Hash::hash(x, state),

            Value::Point(x) => ::core::hash::Hash::hash(x, state),
            Value::Ring(x) => ::core::hash::Hash::hash(x, state),
            Value::Polygon(x) => ::core::hash::Hash::hash(x, state),
            Value::MultiPolygon(x) => ::core::hash::Hash::hash(x, state),

            _ => {}
        }
    }
}

impl Eq for Value {}

impl Value {
    pub fn string(value: impl Into<String>) -> Self {
        Value::String(value.into().into_bytes())
    }

    pub(crate) fn index_value(&self) -> usize {
        match self {
            Value::UInt8(x) => *x as usize,
            Value::UInt16(x) => *x as usize,
            Value::UInt32(x) => *x as usize,
            Value::UInt64(x) => *x as usize,
            _ => unimplemented!(),
        }
    }

    pub fn unwrap_array_ref(&self) -> &[Value] {
        match self {
            Value::Array(a) => &a[..],
            _ => unimplemented!(),
        }
    }

    pub fn unwrap_array(self) -> Vec<Value> {
        match self {
            Value::Array(a) => a,
            _ => unimplemented!(),
        }
    }

    pub fn unwrap_tuple(self) -> Vec<Value> {
        match self {
            Value::Tuple(a) => a,
            _ => unimplemented!(),
        }
    }

    pub fn unarray(self) -> Option<Vec<Value>> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub(crate) fn justify_null_ref<'a>(&'a self, type_: &Type) -> Cow<'a, Value> {
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
        value.to_sql(None)
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
            Value::DateTime64(x) => Type::DateTime64(x.2, x.0),
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

            Value::Point(_) => Type::Point,
            Value::Ring(_) => Type::Ring,
            Value::Polygon(_) => Type::Polygon,
            Value::MultiPolygon(_) => Type::MultiPolygon,
        }
    }
}

fn escape_string(f: &mut fmt::Formatter<'_>, from: impl AsRef<[u8]>) -> fmt::Result {
    let from = from.as_ref();
    for byte in from.iter().copied() {
        if byte < 128 {
            match byte {
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
                _ => write!(f, "{}", Into::<char>::into(byte))?,
            }
        } else {
            write!(f, "\\x{byte:02X}")?;
        }
    }
    Ok(())
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <Self as fmt::Display>::fmt(self, f)
    }
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
                escape_string(f, string)?;
                write!(f, "'")
            }
            Value::Uuid(uuid) => {
                write!(f, "'{}'", uuid)
            }
            Value::Date(date) => {
                let chrono_date: NaiveDate = (*date).into();
                write!(f, "'{}'", chrono_date.format("%Y-%m-%d"))
            }
            Value::DateTime(datetime) => {
                let chrono_date: chrono::DateTime<Tz> =
                    (*datetime).try_into().map_err(|_| fmt::Error)?;
                let string = chrono_date.to_rfc3339_opts(SecondsFormat::AutoSi, true);
                write!(f, "'")?;
                escape_string(f, &string)?;
                write!(f, "'")
            }
            Value::DateTime64(datetime) => {
                let chrono_date: chrono::DateTime<Tz> =
                    FromSql::from_sql(&Type::DateTime64(datetime.2, datetime.0), self.clone())
                        .map_err(|_| fmt::Error)?;
                let string = chrono_date.to_rfc3339_opts(SecondsFormat::AutoSi, true);
                write!(f, "parseDateTime64BestEffort('")?;
                escape_string(f, &string)?;
                write!(f, "', {})", datetime.2)
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
            Value::Point(x) => write!(f, "{:?}", x),
            Value::Ring(x) => write!(f, "{:?}", x),
            Value::Polygon(x) => write!(f, "{:?}", x),
            Value::MultiPolygon(x) => write!(f, "{:?}", x),
        }
    }
}
