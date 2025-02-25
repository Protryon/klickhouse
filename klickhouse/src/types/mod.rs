use std::future::Future;
use std::{fmt::Display, str::FromStr};

pub use chrono_tz::Tz;
use futures_util::FutureExt;
use uuid::Uuid;

mod deserialize;
mod low_cardinality;
mod serialize;
#[cfg(test)]
mod tests;

use crate::{
    i256,
    io::{ClickhouseRead, ClickhouseWrite},
    protocol::MAX_STRING_SIZE,
    u256,
    values::Value,
    Date, DateTime, DynDateTime64, Ipv4, Ipv6, KlickhouseError, Result,
};

/// A raw Clickhouse type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Float32,
    Float64,

    Decimal32(usize),
    Decimal64(usize),
    Decimal128(usize),
    Decimal256(usize),

    String,
    FixedString(usize),

    Uuid,

    Date,
    DateTime(Tz),
    DateTime64(usize, Tz),

    Ipv4,
    Ipv6,

    // Geo types, see
    // https://clickhouse.com/docs/en/sql-reference/data-types/geo
    // These are just aliases of primitive types.
    Point,
    Ring,
    Polygon,
    MultiPolygon,
    /// Not supported
    Enum8(Vec<(String, i8)>),
    /// Not supported
    Enum16(Vec<(String, i16)>),

    LowCardinality(Box<Type>),

    Array(Box<Type>),

    // unused (server never sends this)
    // Nested(IndexMap<String, Type>),
    Tuple(Vec<Type>),

    Nullable(Box<Type>),

    Map(Box<Type>, Box<Type>),
}

impl Type {
    pub fn unwrap_array(&self) -> &Type {
        match self {
            Type::Array(x) => x,
            _ => unimplemented!(),
        }
    }

    pub fn unarray(&self) -> Option<&Type> {
        match self {
            Type::Array(x) => Some(&**x),
            _ => None,
        }
    }

    pub fn unwrap_map(&self) -> (&Type, &Type) {
        match self {
            Type::Map(key, value) => (&**key, &**value),
            _ => unimplemented!(),
        }
    }

    pub fn unmap(&self) -> Option<(&Type, &Type)> {
        match self {
            Type::Map(key, value) => Some((&**key, &**value)),
            _ => None,
        }
    }

    pub fn unwrap_tuple(&self) -> &[Type] {
        match self {
            Type::Tuple(x) => &x[..],
            _ => unimplemented!(),
        }
    }

    pub fn untuple(&self) -> Option<&[Type]> {
        match self {
            Type::Tuple(x) => Some(&x[..]),
            _ => None,
        }
    }

    pub fn unnull(&self) -> Option<&Type> {
        match self {
            Type::Nullable(x) => Some(&**x),
            _ => None,
        }
    }

    pub fn strip_null(&self) -> &Type {
        match self {
            Type::Nullable(x) => x,
            _ => self,
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self, Type::Nullable(_))
    }

    pub fn default_value(&self) -> Value {
        match self {
            Type::Int8 => Value::Int8(0),
            Type::Int16 => Value::Int16(0),
            Type::Int32 => Value::Int32(0),
            Type::Int64 => Value::Int64(0),
            Type::Int128 => Value::Int128(0),
            Type::Int256 => Value::Int256(i256::default()),
            Type::UInt8 => Value::UInt8(0),
            Type::UInt16 => Value::UInt16(0),
            Type::UInt32 => Value::UInt32(0),
            Type::UInt64 => Value::UInt64(0),
            Type::UInt128 => Value::UInt128(0),
            Type::UInt256 => Value::UInt256(u256::default()),
            Type::Float32 => Value::Float32(0.0),
            Type::Float64 => Value::Float64(0.0),
            Type::Decimal32(s) => Value::Decimal32(*s, 0),
            Type::Decimal64(s) => Value::Decimal64(*s, 0),
            Type::Decimal128(s) => Value::Decimal128(*s, 0),
            Type::Decimal256(s) => Value::Decimal256(*s, i256::default()),
            Type::String => Value::String(vec![]),
            Type::FixedString(_) => Value::String(vec![]),
            Type::Uuid => Value::Uuid(Uuid::from_u128(0)),
            Type::Date => Value::Date(Date(0)),
            Type::DateTime(tz) => Value::DateTime(DateTime(*tz, 0)),
            Type::DateTime64(precision, tz) => Value::DateTime64(DynDateTime64(*tz, 0, *precision)),
            Type::Ipv4 => Value::Ipv4(Ipv4::default()),
            Type::Ipv6 => Value::Ipv6(Ipv6::default()),
            Type::Point => Value::Point(Default::default()),
            Type::Ring => Value::Ring(Default::default()),
            Type::Polygon => Value::Polygon(Default::default()),
            Type::MultiPolygon => Value::MultiPolygon(Default::default()),
            Type::Enum8(_) => Value::Enum8(0),
            Type::Enum16(_) => Value::Enum16(0),
            Type::LowCardinality(x) => x.default_value(),
            Type::Array(_) => Value::Array(vec![]),
            // Type::Nested(_) => unimplemented!(),
            Type::Tuple(types) => Value::Tuple(types.iter().map(|x| x.default_value()).collect()),
            Type::Nullable(_) => Value::Null,
            Type::Map(_, _) => Value::Map(vec![], vec![]),
        }
    }

    pub fn strip_low_cardinality(&self) -> &Type {
        match self {
            Type::LowCardinality(x) => x,
            _ => self,
        }
    }
}

// we assume complete identifier normalization and type resolution from clickhouse
fn eat_identifier(input: &str) -> (&str, &str) {
    for (i, c) in input.char_indices() {
        if c.is_alphabetic() || c == '_' || c == '$' || (i > 0 && c.is_numeric()) {
            continue;
        } else {
            return (&input[..i], &input[i..]);
        }
    }
    (input, "")
}

fn parse_args(input: &str) -> Result<Vec<&str>> {
    if !input.starts_with('(') || !input.ends_with(')') {
        return Err(KlickhouseError::TypeParseError(
            "malformed arguments to type".to_string(),
        ));
    }
    let input = input[1..input.len() - 1].trim();
    let mut out = vec![];
    let mut in_parens = 0usize;
    let mut last_start = 0;
    // todo: handle parens in enum strings?
    for (i, c) in input.char_indices() {
        match c {
            ',' => {
                if in_parens == 0 {
                    out.push(input[last_start..i].trim());
                    last_start = i + 1;
                }
            }
            '(' => {
                in_parens += 1;
            }
            ')' => {
                in_parens -= 1;
            }
            _ => (),
        }
    }
    if in_parens != 0 {
        return Err(KlickhouseError::TypeParseError(
            "mismatched parenthesis".to_string(),
        ));
    }
    if last_start != input.len() {
        out.push(input[last_start..input.len()].trim());
    }
    Ok(out)
}

fn parse_scale(from: &str) -> Result<usize> {
    from.parse()
        .map_err(|_| KlickhouseError::TypeParseError("couldn't parse scale".to_string()))
}

fn parse_precision(from: &str) -> Result<usize> {
    from.parse()
        .map_err(|_| KlickhouseError::TypeParseError("couldn't parse precision".to_string()))
}

impl FromStr for Type {
    type Err = KlickhouseError;

    fn from_str(s: &str) -> Result<Self> {
        let (ident, following) = eat_identifier(s);
        if ident.is_empty() {
            return Err(KlickhouseError::TypeParseError(format!(
                "invalid empty identifier for type: '{}'",
                s
            )));
        }
        let following = following.trim();
        if !following.is_empty() {
            let args = parse_args(following)?;
            return Ok(match ident {
                "Decimal" => {
                    if args.len() != 2 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Decimal, expected 2 and got {}",
                            args.len()
                        )));
                    }
                    let p: usize = parse_precision(args[0])?;
                    let s: usize = parse_scale(args[1])?;
                    if p <= 9 {
                        Type::Decimal32(s)
                    } else if p <= 18 {
                        Type::Decimal64(s)
                    } else if p <= 38 {
                        Type::Decimal128(s)
                    } else if p <= 76 {
                        Type::Decimal256(s)
                    } else {
                        return Err(KlickhouseError::TypeParseError(
                            "bad decimal spec, cannot exceed 76 precision".to_string(),
                        ));
                    }
                }
                "Decimal32" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Decimal32, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::Decimal32(parse_scale(args[0])?)
                }
                "Decimal64" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Decimal64, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::Decimal64(parse_scale(args[0])?)
                }
                "Decimal128" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Decimal128, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::Decimal128(parse_scale(args[0])?)
                }
                "Decimal256" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Decimal256, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::Decimal256(parse_scale(args[0])?)
                }
                "FixedString" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for FixedString, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::FixedString(parse_scale(args[0])?)
                }
                "DateTime" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for DateTime, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    if !args[0].starts_with('\'') || !args[0].ends_with('\'') {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "failed to parse timezone for DateTime: '{}'",
                            args[0]
                        )));
                    }
                    Type::DateTime(args[0][1..args[0].len() - 1].parse().map_err(|e| {
                        KlickhouseError::TypeParseError(format!(
                            "failed to parse timezone for DateTime: '{}': {}",
                            args[0], e
                        ))
                    })?)
                }
                "DateTime64" => {
                    if args.len() == 2 {
                        if !args[1].starts_with('\'') || !args[1].ends_with('\'') {
                            return Err(KlickhouseError::TypeParseError(format!(
                                "failed to parse timezone for DateTime64: '{}'",
                                args[0]
                            )));
                        }
                        Type::DateTime64(
                            parse_precision(args[0])?,
                            args[1][1..args[1].len() - 1].parse().map_err(|e| {
                                KlickhouseError::TypeParseError(format!(
                                    "failed to parse timezone for DateTime64: '{}': {}",
                                    args[0], e
                                ))
                            })?,
                        )
                    } else if args.len() == 1 {
                        Type::DateTime64(parse_precision(args[0])?, chrono_tz::UTC)
                    } else {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for DateTime64, expected 1 or 2 and got {}",
                            args.len()
                        )));
                    }
                }
                "Enum8" => {
                    return Err(KlickhouseError::TypeParseError(
                        "unsupported Enum8 type".to_string(),
                    ));
                }
                "Enum16" => {
                    return Err(KlickhouseError::TypeParseError(
                        "unsupported Enum16 type".to_string(),
                    ));
                }
                "LowCardinality" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for LowCardinality, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::LowCardinality(Box::new(Type::from_str(args[0])?))
                }
                "Array" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Array, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::Array(Box::new(Type::from_str(args[0])?))
                }
                "Nested" => {
                    return Err(KlickhouseError::TypeParseError(
                        "unsupported Nested type".to_string(),
                    ));
                }
                "Tuple" => {
                    let mut inner = vec![];
                    for arg in args {
                        inner.push(arg.trim().parse()?);
                    }
                    Type::Tuple(inner)
                }
                "Nullable" => {
                    if args.len() != 1 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Nullable, expected 1 and got {}",
                            args.len()
                        )));
                    }
                    Type::Nullable(Box::new(Type::from_str(args[0])?))
                }
                "Map" => {
                    if args.len() != 2 {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "bad arg count for Map, expected 2 and got {}",
                            args.len()
                        )));
                    }
                    Type::Map(
                        Box::new(Type::from_str(args[0])?),
                        Box::new(Type::from_str(args[1])?),
                    )
                }
                _ => {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "invalid type with arguments: '{}'",
                        ident
                    )))
                }
            });
        }
        Ok(match ident {
            "Int8" => Type::Int8,
            "Int16" => Type::Int16,
            "Int32" => Type::Int32,
            "Int64" => Type::Int64,
            "Int128" => Type::Int128,
            "Int256" => Type::Int256,
            "Bool" | "UInt8" => Type::UInt8,
            "UInt16" => Type::UInt16,
            "UInt32" => Type::UInt32,
            "UInt64" => Type::UInt64,
            "UInt128" => Type::UInt128,
            "UInt256" => Type::UInt256,
            "Float32" => Type::Float32,
            "Float64" => Type::Float64,
            "String" => Type::String,
            "UUID" => Type::Uuid,
            "Date" => Type::Date,
            "DateTime" => Type::DateTime(chrono_tz::UTC),
            "IPv4" => Type::Ipv4,
            "IPv6" => Type::Ipv6,
            "Point" => Type::Point,
            "Ring" => Type::Ring,
            "Polygon" => Type::Polygon,
            "MultiPolygon" => Type::MultiPolygon,
            _ => {
                return Err(KlickhouseError::TypeParseError(format!(
                    "invalid type name: '{}'",
                    ident
                )))
            }
        })
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Int8 => write!(f, "Int8"),
            Type::Int16 => write!(f, "Int16"),
            Type::Int32 => write!(f, "Int32"),
            Type::Int64 => write!(f, "Int64"),
            Type::Int128 => write!(f, "Int128"),
            Type::Int256 => write!(f, "Int256"),
            Type::UInt8 => write!(f, "UInt8"),
            Type::UInt16 => write!(f, "UInt16"),
            Type::UInt32 => write!(f, "UInt32"),
            Type::UInt64 => write!(f, "UInt64"),
            Type::UInt128 => write!(f, "UInt128"),
            Type::UInt256 => write!(f, "UInt256"),
            Type::Float32 => write!(f, "Float32"),
            Type::Float64 => write!(f, "Float64"),
            Type::Decimal32(s) => write!(f, "Decimal32({})", s),
            Type::Decimal64(s) => write!(f, "Decimal64({})", s),
            Type::Decimal128(s) => write!(f, "Decimal128({})", s),
            Type::Decimal256(s) => write!(f, "Decimal256({})", s),
            Type::String => write!(f, "String"),
            Type::FixedString(s) => write!(f, "FixedString({})", s),
            Type::Uuid => write!(f, "UUID"),
            Type::Date => write!(f, "Date"),
            Type::DateTime(tz) => write!(f, "DateTime('{}')", tz),
            Type::DateTime64(precision, tz) => write!(f, "DateTime64({},'{}')", precision, tz),
            Type::Ipv4 => write!(f, "IPv4"),
            Type::Ipv6 => write!(f, "IPv6"),
            Type::Point => write!(f, "Point"),
            Type::Ring => write!(f, "Ring"),
            Type::Polygon => write!(f, "Polygon"),
            Type::MultiPolygon => write!(f, "MultiPolygon"),
            Type::Enum8(items) => write!(
                f,
                "Enum8({})",
                items
                    .iter()
                    .map(|(name, value)| format!("{}={}", name, value))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Type::Enum16(items) => write!(
                f,
                "Enum16({})",
                items
                    .iter()
                    .map(|(name, value)| format!("{}={}", name, value))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Type::LowCardinality(inner) => write!(f, "LowCardinality({})", inner),
            Type::Array(inner) => write!(f, "Array({})", inner),
            // Type::Nested(items) => format!("Nested({})", items.iter().map(|(key, value)| format!("{} {}", key, value.to_string())).collect::<Vec<_>>().join(",")),
            Type::Tuple(items) => write!(
                f,
                "Tuple({})",
                items
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Type::Nullable(inner) => write!(f, "Nullable({})", inner),
            Type::Map(key, value) => write!(f, "Map({},{})", key, value),
        }
    }
}

impl Type {
    pub(crate) fn deserialize_prefix<'a, R: ClickhouseRead>(
        &'a self,
        reader: &'a mut R,
        state: &'a mut DeserializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use deserialize::*;

        async move {
            match self {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256
                | Type::Float32
                | Type::Float64
                | Type::Decimal32(_)
                | Type::Decimal64(_)
                | Type::Decimal128(_)
                | Type::Decimal256(_)
                | Type::Uuid
                | Type::Date
                | Type::DateTime(_)
                | Type::DateTime64(_, _)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Enum8(_)
                | Type::Enum16(_) => {
                    sized::SizedDeserializer::read_prefix(self, reader, state).await?
                }

                Type::String | Type::FixedString(_) => {
                    string::StringDeserializer::read_prefix(self, reader, state).await?
                }

                Type::Array(_) => {
                    array::ArrayDeserializer::read_prefix(self, reader, state).await?
                }
                Type::Tuple(_) => {
                    tuple::TupleDeserializer::read_prefix(self, reader, state).await?
                }
                Type::Point => geo::PointDeserializer::read_prefix(self, reader, state).await?,
                Type::Ring => geo::RingDeserializer::read_prefix(self, reader, state).await?,
                Type::Polygon => geo::PolygonDeserializer::read_prefix(self, reader, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonDeserializer::read_prefix(self, reader, state).await?
                }
                Type::Nullable(_) => {
                    nullable::NullableDeserializer::read_prefix(self, reader, state).await?
                }
                Type::Map(_, _) => map::MapDeserializer::read_prefix(self, reader, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalityDeserializer::read_prefix(self, reader, state)
                        .await?
                }
            }
            Ok(())
        }
        .boxed()
    }

    pub(crate) fn deserialize_column<'a, R: ClickhouseRead>(
        &'a self,
        reader: &'a mut R,
        rows: usize,
        state: &'a mut DeserializerState,
    ) -> impl Future<Output = Result<Vec<Value>>> + Send + 'a {
        use deserialize::*;

        async move {
            if rows > MAX_STRING_SIZE {
                return Err(KlickhouseError::ProtocolError(format!(
                    "deserialize response size too large. {} > {}",
                    rows, MAX_STRING_SIZE
                )));
            }

            Ok(match self {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256
                | Type::Float32
                | Type::Float64
                | Type::Decimal32(_)
                | Type::Decimal64(_)
                | Type::Decimal128(_)
                | Type::Decimal256(_)
                | Type::Uuid
                | Type::Date
                | Type::DateTime(_)
                | Type::DateTime64(_, _)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Enum8(_)
                | Type::Enum16(_) => {
                    sized::SizedDeserializer::read(self, reader, rows, state).await?
                }

                Type::String | Type::FixedString(_) => {
                    string::StringDeserializer::read(self, reader, rows, state).await?
                }

                Type::Array(_) => array::ArrayDeserializer::read(self, reader, rows, state).await?,
                Type::Ring => geo::RingDeserializer::read(self, reader, rows, state).await?,
                Type::Polygon => geo::PolygonDeserializer::read(self, reader, rows, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonDeserializer::read(self, reader, rows, state).await?
                }
                Type::Tuple(_) => tuple::TupleDeserializer::read(self, reader, rows, state).await?,
                Type::Point => geo::PointDeserializer::read(self, reader, rows, state).await?,
                Type::Nullable(_) => {
                    nullable::NullableDeserializer::read(self, reader, rows, state).await?
                }
                Type::Map(_, _) => map::MapDeserializer::read(self, reader, rows, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalityDeserializer::read(self, reader, rows, state)
                        .await?
                }
            })
        }
        .boxed()
    }

    pub(crate) fn serialize_column<'a, W: ClickhouseWrite>(
        &'a self,
        values: Vec<Value>,
        writer: &'a mut W,
        state: &'a mut SerializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use serialize::*;

        async move {
            match self {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256
                | Type::Float32
                | Type::Float64
                | Type::Decimal32(_)
                | Type::Decimal64(_)
                | Type::Decimal128(_)
                | Type::Decimal256(_)
                | Type::Uuid
                | Type::Date
                | Type::DateTime(_)
                | Type::DateTime64(_, _)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Enum8(_)
                | Type::Enum16(_) => {
                    sized::SizedSerializer::write(self, values, writer, state).await?
                }

                Type::String | Type::FixedString(_) => {
                    string::StringSerializer::write(self, values, writer, state).await?
                }

                Type::Array(_) => {
                    array::ArraySerializer::write(self, values, writer, state).await?
                }
                Type::Tuple(_) => {
                    tuple::TupleSerializer::write(self, values, writer, state).await?
                }
                Type::Point => geo::PointSerializer::write(self, values, writer, state).await?,
                Type::Ring => geo::RingSerializer::write(self, values, writer, state).await?,
                Type::Polygon => geo::PolygonSerializer::write(self, values, writer, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonSerializer::write(self, values, writer, state).await?
                }
                Type::Nullable(_) => {
                    nullable::NullableSerializer::write(self, values, writer, state).await?
                }
                Type::Map(_, _) => map::MapSerializer::write(self, values, writer, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalitySerializer::write(self, values, writer, state)
                        .await?
                }
            }
            Ok(())
        }
        .boxed()
    }

    pub(crate) fn serialize_prefix<'a, W: ClickhouseWrite>(
        &'a self,
        writer: &'a mut W,
        state: &'a mut SerializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use serialize::*;

        async move {
            match self {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256
                | Type::Float32
                | Type::Float64
                | Type::Decimal32(_)
                | Type::Decimal64(_)
                | Type::Decimal128(_)
                | Type::Decimal256(_)
                | Type::Uuid
                | Type::Date
                | Type::DateTime(_)
                | Type::DateTime64(_, _)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Enum8(_)
                | Type::Enum16(_) => {
                    sized::SizedSerializer::write_prefix(self, writer, state).await?
                }

                Type::String | Type::FixedString(_) => {
                    string::StringSerializer::write_prefix(self, writer, state).await?
                }

                Type::Array(_) => array::ArraySerializer::write_prefix(self, writer, state).await?,
                Type::Tuple(_) => tuple::TupleSerializer::write_prefix(self, writer, state).await?,
                Type::Point => geo::PointSerializer::write_prefix(self, writer, state).await?,
                Type::Ring => geo::RingSerializer::write_prefix(self, writer, state).await?,
                Type::Polygon => geo::PolygonSerializer::write_prefix(self, writer, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonSerializer::write_prefix(self, writer, state).await?
                }
                Type::Nullable(_) => {
                    nullable::NullableSerializer::write_prefix(self, writer, state).await?
                }
                Type::Map(_, _) => map::MapSerializer::write_prefix(self, writer, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalitySerializer::write_prefix(self, writer, state)
                        .await?
                }
            }
            Ok(())
        }
        .boxed()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Type::Decimal32(precision) => {
                if *precision == 0 || *precision > 9 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "precision out of bounds for Decimal32({}) must be in range (1..=9)",
                        *precision
                    )));
                }
            }
            Type::DateTime64(precision, _) | Type::Decimal64(precision) => {
                if *precision == 0 || *precision > 18 {
                    return Err(KlickhouseError::TypeParseError(format!("precision out of bounds for Decimal64/DateTime64({}) must be in range (1..=18)", *precision)));
                }
            }
            Type::Decimal128(precision) => {
                if *precision == 0 || *precision > 38 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "precision out of bounds for Decimal128({}) must be in range (1..=38)",
                        *precision
                    )));
                }
            }
            Type::Decimal256(precision) => {
                if *precision == 0 || *precision > 76 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "precision out of bounds for Decimal256({}) must be in range (1..=76)",
                        *precision
                    )));
                }
            }
            Type::LowCardinality(inner) => match inner.strip_null() {
                Type::String
                | Type::FixedString(_)
                | Type::Date
                | Type::DateTime(_)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256 => inner.validate()?,
                _ => {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "illegal type '{:?}' in LowCardinality, not allowed",
                        inner
                    )))
                }
            },
            Type::Array(inner) => {
                inner.validate()?;
            }
            // Type::Nested(_) => return Err(anyhow!("nested not implemented")),
            Type::Tuple(inner) => {
                for inner in inner {
                    inner.validate()?;
                }
            }
            Type::Nullable(inner) => {
                match &**inner {
                    Type::Array(_)
                    | Type::Map(_, _)
                    | Type::LowCardinality(_)
                    | Type::Tuple(_)
                    | Type::Nullable(_) => {
                        /*  | Type::Nested(_) */
                        return Err(KlickhouseError::TypeParseError(format!(
                            "nullable cannot contain composite type '{:?}'",
                            inner
                        )));
                    }
                    _ => inner.validate()?,
                }
            }
            Type::Map(key, value) => {
                if !matches!(
                    &**key,
                    Type::String
                        | Type::FixedString(_)
                        | Type::Int8
                        | Type::Int16
                        | Type::Int32
                        | Type::Int64
                        | Type::Int128
                        | Type::Int256
                        | Type::UInt8
                        | Type::UInt16
                        | Type::UInt32
                        | Type::UInt64
                        | Type::UInt128
                        | Type::UInt256
                        | Type::LowCardinality(_)
                        | Type::Uuid
                        | Type::Date
                        | Type::DateTime(_)
                        | Type::DateTime64(_, _)
                        | Type::Enum8(_)
                        | Type::Enum16(_)
                ) {
                    return Err(KlickhouseError::TypeParseError("key in map must be String, Integer, LowCardinality, FixedString, UUID, Date, DateTime, Date32, Enum".to_string()));
                }
                key.validate()?;
                value.validate()?;
            }
            _ => (),
        }
        Ok(())
    }

    pub(crate) fn validate_value(&self, value: &Value) -> Result<()> {
        self.validate()?;
        if !self.inner_validate_value(value) {
            return Err(KlickhouseError::TypeParseError(format!(
                "could not assign value '{:?}' to type '{:?}'",
                value, self
            )));
        }
        Ok(())
    }

    fn inner_validate_value(&self, value: &Value) -> bool {
        match (self, value) {
            (Type::Int8, Value::Int8(_))
            //FIXME: this is for compatibility with bools in CH < 22
            | (Type::Int8, Value::UInt8(_))
            | (Type::Int16, Value::Int16(_))
            | (Type::Int32, Value::Int32(_))
            | (Type::Int64, Value::Int64(_))
            | (Type::Int128, Value::Int128(_))
            | (Type::Int256, Value::Int256(_))
            | (Type::UInt8, Value::UInt8(_))
            | (Type::UInt16, Value::UInt16(_))
            | (Type::UInt32, Value::UInt32(_))
            | (Type::UInt64, Value::UInt64(_))
            | (Type::UInt128, Value::UInt128(_))
            | (Type::UInt256, Value::UInt256(_))
            | (Type::Float32, Value::Float32(_))
            | (Type::Float64, Value::Float64(_)) => true,
            (Type::Decimal32(precision1), Value::Decimal32(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal64(precision1), Value::Decimal64(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal128(precision1), Value::Decimal128(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal256(precision1), Value::Decimal256(precision2, _)) => {
                precision1 == precision2
            }
            (Type::FixedString(_), Value::Array(items))
            | (Type::String, Value::Array(items)) if items.iter().all(|item| matches!(item, Value::UInt8(_) | Value::Int8(_)))
            => true,
            (Type::String, Value::String(_))
            | (Type::FixedString(_), Value::String(_))
            | (Type::Uuid, Value::Uuid(_))
            | (Type::Date, Value::Date(_)) => true,
            (Type::DateTime(tz1), Value::DateTime(date)) => tz1 == &date.0,
            (Type::DateTime64(precision1, tz1), Value::DateTime64(tz2)) => {
                tz1 == &tz2.0 && precision1 == &tz2.2
            }
            (Type::Ipv4, Value::Ipv4(_)) | (Type::Ipv6, Value::Ipv6(_)) => true,
            (Type::Point, Value::Point(_)) | (Type::Ring, Value::Ring(_)) | (Type::Polygon, Value::Polygon(_)) | (Type::MultiPolygon, Value::MultiPolygon(_)) => true,
            (Type::Enum8(entries), Value::Enum8(index)) => entries.iter().any(|x| x.1 == *index),
            (Type::Enum16(entries), Value::Enum16(index)) => entries.iter().any(|x| x.1 == *index),
            (Type::LowCardinality(x), value) => x.inner_validate_value(value),
            (Type::Array(inner_type), Value::Array(values)) => {
                values.iter().all(|x| inner_type.inner_validate_value(x))
            }
            (Type::Tuple(inner_types), Value::Tuple(values)) => inner_types
                .iter()
                .zip(values.iter())
                .all(|(type_, value)| type_.inner_validate_value(value)),
            (Type::Nullable(inner), value) => {
                value == &Value::Null || inner.inner_validate_value(value)
            }
            (Type::Map(key, value), Value::Map(keys, values)) => {
                keys.iter().all(|x| key.inner_validate_value(x))
                    && values.iter().all(|x| value.inner_validate_value(x))
            }
            (_, _) => false,
        }
    }
}

pub struct DeserializerState {}

pub struct SerializerState {}

pub trait Deserializer {
    fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _state: &mut DeserializerState,
    ) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }

    fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> impl Future<Output = Result<Vec<Value>>>;
}

pub trait Serializer {
    fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }

    fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> impl Future<Output = Result<()>>;
}
