use anyhow::*;
use tokio::io::{AsyncWriteExt};

use crate::{io::ClickhouseWrite, values::Value};

use super::{Serializer, SerializerState, Type};

pub struct SizedSerializer;

fn swap_endian_256(mut input: [u8; 32]) -> [u8; 32] {
    input.reverse();
    input
}

#[async_trait::async_trait]
impl Serializer for SizedSerializer {
    async fn write<W: ClickhouseWrite>(type_: &Type, value: &Value, writer: &mut W, _state: &mut SerializerState) -> Result<()> {
        match value.justify_null(type_).as_ref() {
            Value::Int8(x) => writer.write_i8(*x).await?,
            Value::Int16(x) => writer.write_i16_le(*x).await?,
            Value::Int32(x) => writer.write_i32_le(*x).await?,
            Value::Int64(x) => writer.write_i64_le(*x).await?,
            Value::Int128(x) => writer.write_i128_le(*x).await?,
            Value::Int256(x) => writer.write_all(&swap_endian_256(x.0)[..]).await?,
            Value::UInt8(x) => writer.write_u8(*x).await?,
            Value::UInt16(x) => writer.write_u16_le(*x).await?,
            Value::UInt32(x) => writer.write_u32_le(*x).await?,
            Value::UInt64(x) => writer.write_u64_le(*x).await?,
            Value::UInt128(x) => writer.write_u128_le(*x).await?,
            Value::UInt256(x) => writer.write_all(&swap_endian_256(x.0)[..]).await?,
            Value::Float32(x) => writer.write_u32_le(*x).await?,
            Value::Float64(x) => writer.write_u64_le(*x).await?,
            Value::Decimal32(_, x) => writer.write_i32_le(*x).await?,
            Value::Decimal64(_, x) => writer.write_i64_le(*x).await?,
            Value::Decimal128(_, x) => writer.write_i128_le(*x).await?,
            Value::Decimal256(_, x) => writer.write_all(&swap_endian_256(x.0)[..]).await?,
            Value::Uuid(x) => {
                let n = x.as_u128();
                let n1 = (n >> 64) as u64;
                let n2 = n as u64;
                writer.write_u64_le(n1).await?;
                writer.write_u64_le(n2).await?;
            },
            Value::Date(x) => writer.write_u16_le(x.0).await?,
            Value::DateTime(x) => writer.write_u32_le(x.1).await?,
            Value::DateTime64(_, _, x) => writer.write_u64_le(*x).await?,
            Value::Enum8(x) => writer.write_u8(*x).await?,
            Value::Enum16(x) => writer.write_u16_le(*x).await?,
            _ => unimplemented!(),
        }
        Ok(())
    }
}