use std::{collections::VecDeque, fmt, iter::Peekable, marker::PhantomData};

use indexmap::IndexMap;
use serde::{Deserializer, de::{DeserializeSeed, MapAccess, SeqAccess, Visitor}};
use anyhow::*;

use crate::types::Type;

use super::Value;

pub trait AnyDeserializer<'de> {
    type Error: serde::de::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error>;
}

pub struct AnyDeserializerItem<'de, D: AnyDeserializer<'de>>(D, PhantomData<&'de ()>);

impl<'de, D: AnyDeserializer<'de>> AnyDeserializerItem<'de, D> {
    pub fn new(item: D) -> Self {
        Self(item, PhantomData)
    }
}

impl<'de, T: AnyDeserializer<'de>> Deserializer<'de> for AnyDeserializerItem<'de, T> {
    type Error = T::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.0.deserialize_any(visitor)
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }
}

pub struct ClickhouseDeserializer<'de> {
    pub type_: &'de Type,
    pub value: &'de Value,
}

pub struct ClickhouseOwnedDeserializer {
    pub value: Value,
    pub type_: Type,
}

pub struct ClickhouseRowDeserializer {
    pub items: IndexMap<String, (Type, Value)>,
}

pub struct DeserializerError(anyhow::Error);

impl fmt::Display for DeserializerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for DeserializerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for DeserializerError {}

impl serde::de::Error for DeserializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(anyhow!("{}", msg))
    }
}

struct ArrayAccess<'de>(&'de Type, &'de [Value]);

impl<'de> SeqAccess<'de> for ArrayAccess<'de> {
    type Error = DeserializerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        let element = match self.1.first() {
            Some(x) => x,
            None => return Ok(None),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseDeserializer {
            value: element,
            type_: self.0,
        })).map(Some);
        self.1 = &self.1[1..];
        deserialized
    }
}

struct TupleAccess<'de>(&'de [Type], &'de [Value]);

impl<'de> SeqAccess<'de> for TupleAccess<'de> {
    type Error = DeserializerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        let element = match self.1.first() {
            Some(x) => x,
            None => return Ok(None),
        };
        let type_ = match self.0.first() {
            Some(x) => x,
            None => return Ok(None),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseDeserializer {
            value: element,
            type_,
        })).map(Some);
        self.0 = &self.0[1..];
        self.1 = &self.1[1..];
        deserialized
    }
}

struct ArrayOwnedAccess(Type, VecDeque<Value>);

impl<'de> SeqAccess<'de> for ArrayOwnedAccess {
    type Error = DeserializerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        let element = match self.1.pop_front() {
            Some(x) => x,
            None => return Ok(None),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseOwnedDeserializer {
            value: element,
            type_: self.0.clone(),
        })).map(Some);
        deserialized
    }
}

struct TupleOwnedAccess(VecDeque<Type>, VecDeque<Value>);

impl<'de> SeqAccess<'de> for TupleOwnedAccess {
    type Error = DeserializerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        let element = match self.1.pop_front() {
            Some(x) => x,
            None => return Ok(None),
        };
        let type_ = match self.0.pop_front() {
            Some(x) => x,
            None => return Ok(None),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseOwnedDeserializer {
            value: element,
            type_,
        })).map(Some);
        deserialized
    }
}

struct MapAccessor<'de> {
    key_type: &'de Type,
    keys: &'de [Value],
    value_type: &'de Type,
    values: &'de [Value],
}

impl<'de> MapAccess<'de> for MapAccessor<'de> {
    type Error = DeserializerError;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error> {
        let key = match self.keys.first() {
            Some(x) => x,
            None => return Ok(None),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseDeserializer {
            value: key,
            type_: self.key_type,
        })).map(Some);
        deserialized
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Self::Error> {
        let value = match self.values.first() {
            Some(x) => x,
            None => panic!("called next_value at eof"),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseDeserializer {
            value,
            type_: self.value_type,
        }));
        self.keys = &self.keys[1..];
        self.values = &self.values[1..];
        deserialized
    }
}


struct MapOwnedAccessor {
    key_type: Type,
    keys: VecDeque<Value>,
    value_type: Type,
    values: VecDeque<Value>,
}

impl<'de> MapAccess<'de> for MapOwnedAccessor {
    type Error = DeserializerError;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error> {
        let key = match self.keys.pop_front() {
            Some(x) => x,
            None => return Ok(None),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseOwnedDeserializer {
            value: key,
            type_: self.key_type.clone(),
        })).map(Some);
        deserialized
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Self::Error> {
        let value = match self.values.pop_front() {
            Some(x) => x,
            None => panic!("called next_value at eof"),
        };
        let deserialized = seed.deserialize(AnyDeserializerItem::new(ClickhouseOwnedDeserializer {
            value,
            type_: self.value_type.clone(),
        }));
        deserialized
    }
}

impl<'de> AnyDeserializer<'de> for ClickhouseDeserializer<'de> {
    type Error = DeserializerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        if let Type::Nullable(inner) = &self.type_ {
            if self.value != &Value::Null {
                return visitor.visit_some(AnyDeserializerItem::new(ClickhouseDeserializer {
                    value: self.value,
                    type_: &**inner
                }))
            }
        }
        match self.value {
            Value::Int8(x) => visitor.visit_i8(*x),
            Value::Int16(x) => visitor.visit_i16(*x),
            Value::Int32(x) => visitor.visit_i32(*x),
            Value::Int64(x) => visitor.visit_i64(*x),
            Value::Int128(x) => visitor.visit_i128(*x),
            Value::Int256(x) => visitor.visit_borrowed_bytes(&x[..]),
            Value::UInt8(x) => visitor.visit_u8(*x),
            Value::UInt16(x) => visitor.visit_u16(*x),
            Value::UInt32(x) => visitor.visit_u32(*x),
            Value::UInt64(x) => visitor.visit_u64(*x),
            Value::UInt128(x) => visitor.visit_u128(*x),
            Value::UInt256(x) => visitor.visit_borrowed_bytes(&x[..]),
            Value::Float32(x) => visitor.visit_f32(f32::from_bits(*x)),
            Value::Float64(x) => visitor.visit_f64(f64::from_bits(*x)),
            Value::Decimal32(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int32].into(), vec![Value::UInt64(*s as u64), Value::Int32(*x)].into())),
            Value::Decimal64(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int64].into(), vec![Value::UInt64(*s as u64), Value::Int64(*x)].into())),
            Value::Decimal128(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int128].into(), vec![Value::UInt64(*s as u64), Value::Int128(*x)].into())),
            Value::Decimal256(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int256].into(), vec![Value::UInt64(*s as u64), Value::Int256(*x)].into())),
            Value::String(x) => visitor.visit_str(&**x),
            Value::Uuid(u) => visitor.visit_string(u.to_string()),
            Value::Date(x) => visitor.visit_u16(*x),
            Value::DateTime(tz, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::String, Type::UInt32].into(), vec![Value::String(tz.to_string()), Value::UInt32(*x)].into())),
            Value::DateTime64(tz, precision, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::String, Type::UInt64, Type::UInt64].into(), vec![Value::String(tz.to_string()), Value::UInt64(*precision as u64), Value::UInt64(*x)].into())),
            Value::Enum8(_) => unimplemented!(),
            Value::Enum16(_) => unimplemented!(),
            Value::Array(items) => visitor.visit_seq(ArrayAccess(self.type_.unwrap_array(), &items[..])),
            // Value::Nested(items) => visitor.visit_map(IndexMapAccessor(items.iter().peekable())),
            Value::Tuple(items) => visitor.visit_seq(TupleAccess(self.type_.unwrap_tuple(), &items[..])),
            Value::Null => visitor.visit_none(),
            Value::Map(keys, values) => {
                let (key_type, value_type) = self.type_.unwrap_map();
                visitor.visit_map(MapAccessor {
                    key_type,
                    keys: &keys[..],
                    value_type,
                    values: &values[..],
                })
            },
            Value::_Marker(_) | Value::_Bytes(_) => unimplemented!(),
        }
    }
}

impl<'de> AnyDeserializer<'de> for ClickhouseOwnedDeserializer {
    type Error = DeserializerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        if let Type::Nullable(inner) = &self.type_ {
            if self.value != Value::Null {
                return visitor.visit_some(AnyDeserializerItem::new(ClickhouseOwnedDeserializer {
                    value: self.value,
                    type_: (&**inner).clone(),
                }))
            }
        }
        match self.value {
            Value::Int8(x) => visitor.visit_i8(x),
            Value::Int16(x) => visitor.visit_i16(x),
            Value::Int32(x) => visitor.visit_i32(x),
            Value::Int64(x) => visitor.visit_i64(x),
            Value::Int128(x) => visitor.visit_i128(x),
            Value::Int256(x) => visitor.visit_bytes(&x[..]),
            Value::UInt8(x) => visitor.visit_u8(x),
            Value::UInt16(x) => visitor.visit_u16(x),
            Value::UInt32(x) => visitor.visit_u32(x),
            Value::UInt64(x) => visitor.visit_u64(x),
            Value::UInt128(x) => visitor.visit_u128(x),
            Value::UInt256(x) => visitor.visit_bytes(&x[..]),
            Value::Float32(x) => visitor.visit_f32(f32::from_bits(x)),
            Value::Float64(x) => visitor.visit_f64(f64::from_bits(x)),
            Value::Decimal32(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int32].into(), vec![Value::UInt64(s as u64), Value::Int32(x)].into())),
            Value::Decimal64(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int64].into(), vec![Value::UInt64(s as u64), Value::Int64(x)].into())),
            Value::Decimal128(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int128].into(), vec![Value::UInt64(s as u64), Value::Int128(x)].into())),
            Value::Decimal256(s, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::UInt64, Type::Int256].into(), vec![Value::UInt64(s as u64), Value::Int256(x)].into())),
            Value::String(x) => visitor.visit_string(x),
            Value::Uuid(u) => visitor.visit_string(u.to_string()),
            Value::Date(x) => visitor.visit_u16(x),
            Value::DateTime(tz, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::String, Type::UInt32].into(), vec![Value::String(tz.to_string()), Value::UInt32(x)].into())),
            Value::DateTime64(tz, precision, x) => visitor.visit_seq(TupleOwnedAccess(vec![Type::String, Type::UInt64, Type::UInt64].into(), vec![Value::String(tz.to_string()), Value::UInt64(precision as u64), Value::UInt64(x)].into())),
            Value::Enum8(_) => unimplemented!(),
            Value::Enum16(_) => unimplemented!(),
            Value::Array(items) => visitor.visit_seq(ArrayOwnedAccess(self.type_.unwrap_array().clone(), items.into())),
            // Value::Nested(items) => visitor.visit_map(IndexMapOwnedAccessor(items.into_iter().peekable())),
            Value::Tuple(items) => visitor.visit_seq(TupleOwnedAccess(self.type_.unwrap_tuple().iter().cloned().collect(), items.into())),
            Value::Null => visitor.visit_none(),
            Value::Map(keys, values) => {
                let (key_type, value_type) = self.type_.unwrap_map();
                visitor.visit_map(MapOwnedAccessor {
                    key_type: key_type.clone(),
                    keys: keys.into(),
                    value_type: value_type.clone(),
                    values: values.into(),
                })
            },
            Value::_Marker(_) | Value::_Bytes(_) => unimplemented!(),
        }
    }
}

struct IndexMapOwnedAccessor(Peekable<indexmap::map::IntoIter<String, (Type, Value)>>);

impl<'de> MapAccess<'de> for IndexMapOwnedAccessor {
    type Error = DeserializerError;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error> {
        match self.0.peek() {
            Some(x) => seed.deserialize(AnyDeserializerItem(ClickhouseOwnedDeserializer {
                value: Value::String(x.0.clone()),
                type_: Type::String,
            }, PhantomData)).map(Some),
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Self::Error> {
        match self.0.next() {
            Some(x) => seed.deserialize(AnyDeserializerItem(ClickhouseOwnedDeserializer {
                value: x.1.1,
                type_: x.1.0,
            }, PhantomData)),
            None => unimplemented!(),
        }
    }
}

impl<'de> AnyDeserializer<'de> for ClickhouseRowDeserializer {
    type Error = DeserializerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_map(IndexMapOwnedAccessor(self.items.into_iter().peekable()))
    }
}

pub struct ClickhouseStrDeserializer<'de> {
    pub value: &'de str,
}

impl<'de> AnyDeserializer<'de> for ClickhouseStrDeserializer<'de> {
    type Error = DeserializerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_str(self.value)
    }
}

pub struct ClickhouseStringDeserializer {
    pub value: String,
}

impl<'de> AnyDeserializer<'de> for ClickhouseStringDeserializer {
    type Error = DeserializerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_string(self.value)
    }
}
