use std::fmt;

use serde::{Serialize, Serializer, ser::{SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant}};
use anyhow::*;
use uuid::Uuid;

use crate::types::Type;

use super::Value;

#[derive(Serialize)]
pub(crate) struct _KlickhouseMarker(pub String);

pub struct ClickhouseSerializer;

pub struct SerializerError(anyhow::Error);

impl fmt::Display for SerializerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for SerializerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for SerializerError {}

impl serde::ser::Error for SerializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(anyhow!("{}", msg))
    }
}


impl Serializer for ClickhouseSerializer {
    type Ok = Value;

    type Error = SerializerError;

    type SerializeSeq = ArraySerializer;

    type SerializeTuple = TupleSerializer;

    type SerializeTupleStruct = TupleStructSerializer;

    type SerializeTupleVariant = TupleVariantSerializer;

    type SerializeMap = MapSerializer;

    type SerializeStruct = StructSerializer;

    type SerializeStructVariant = StructVariantSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.serialize_u8(if v { 1 } else { 0 })
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int64(v))
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int128(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt64(v))
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        Ok(Value::UInt128(v))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Float32(v.to_bits()))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Float64(v.to_bits()))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        if v.len() == 32 {
            let mut buf = [0u8; 32];
            buf.copy_from_slice(v);
            return Ok(Value::_Bytes(buf));
        }
        unimplemented!("no representation for bytes in clickhouse")
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Null)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        Ok(value.serialize(self)?)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Tuple(vec![]))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        unimplemented!("no representation for unit struct in clickhouse")
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        unimplemented!("no representation for unit variant in clickhouse")
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        let serialized = value.serialize(self)?;
        if name == "_KlickhouseMarker" {
            let type_name = match serialized {
                Value::String(s) => s,
                _ => unimplemented!(),
            };
            let type_: Type = type_name.parse().map_err(SerializerError)?;
            return Ok(Value::_Marker(type_));
        }
        Ok(serialized)
    }

    fn serialize_newtype_variant<T: serde::Serialize + ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        todo!("enums not supported")
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(ArraySerializer(vec![]))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(TupleSerializer(vec![]))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(TupleStructSerializer(vec![]))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(TupleVariantSerializer)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(MapSerializer(vec![], vec![]))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(StructSerializer)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(StructVariantSerializer)
    }
}

pub struct ArraySerializer(Vec<Value>);

impl SerializeSeq for ArraySerializer {
    type Ok = Value;
    type Error = SerializerError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let value = value.serialize(ClickhouseSerializer)?;
        self.0.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Array(self.0))
    }
}
pub struct TupleSerializer(Vec<Value>);

impl SerializeTuple for TupleSerializer {
    type Ok = Value;
    type Error = SerializerError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let value = value.serialize(ClickhouseSerializer)?;
        self.0.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if !self.0.is_empty() {
            if let Value::_Marker(type_) = &self.0[0] {
                let target = &self.0[1];
                return Ok(match type_ {
                    Type::Decimal32(precision) => Value::Decimal32(*precision, match target {
                        Value::Int32(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::Decimal64(precision) => Value::Decimal64(*precision, match target {
                        Value::Int64(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::Decimal128(precision) => Value::Decimal128(*precision, match target {
                        Value::Int128(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::Decimal256(precision) => Value::Decimal256(*precision, match target {
                        Value::Int256(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::Int256 => Value::Int256(match target {
                        Value::_Bytes(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::UInt256 => Value::UInt256(match target {
                        Value::_Bytes(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::Date => Value::Date(match target {
                        Value::UInt16(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::DateTime(tz) => Value::DateTime(*tz, match target {
                        Value::UInt32(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::DateTime64(precision, tz) => Value::DateTime64(*tz, *precision, match target {
                        Value::UInt64(x) => *x,
                        _ => unimplemented!(),
                    }),
                    Type::Uuid => Value::Uuid(match target {
                        Value::String(x) => x.parse::<Uuid>().map_err(|e| SerializerError(e.into()))?,
                        _ => unimplemented!(),
                    }),
                    _ => unimplemented!(),
                })
            }
        }
        Ok(Value::Tuple(self.0))
    }
}

pub struct TupleStructSerializer(Vec<Value>);

impl SerializeTupleStruct for TupleStructSerializer {
    type Ok = Value;
    type Error = SerializerError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let value = value.serialize(ClickhouseSerializer)?;
        self.0.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Tuple(self.0))
    }
}

pub struct TupleVariantSerializer;

impl SerializeTupleVariant for TupleVariantSerializer {
    type Ok = Value;
    type Error = SerializerError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        unimplemented!("no representation for tagged enum variant in clickhouse");
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("no representation for tagged enum variant in clickhouse");
    }
}

pub struct MapSerializer(Vec<Value>, Vec<Value>);

impl SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = SerializerError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        let value = key.serialize(ClickhouseSerializer)?;
        self.0.push(value);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let value = value.serialize(ClickhouseSerializer)?;
        self.1.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Map(self.0, self.1))
    }
}


pub struct StructSerializer;

impl SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = SerializerError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

pub struct StructVariantSerializer;

impl SerializeStructVariant for StructVariantSerializer {
    type Ok = Value;
    type Error = SerializerError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        unimplemented!("no representation for tagged enum variant in clickhouse");
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("no representation for tagged enum variant in clickhouse");
    }
}