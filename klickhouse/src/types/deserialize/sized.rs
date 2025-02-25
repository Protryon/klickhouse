use std::net::{Ipv4Addr, Ipv6Addr};

use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{i256, io::ClickhouseRead, u256, values::Value, Date, DateTime, DynDateTime64, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct SizedDeserializer;

impl Deserializer for SizedDeserializer {
    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(rows);
        for _ in 0..rows {
            out.push(match type_ {
                Type::Int8 => Value::Int8(reader.read_i8().await?),
                Type::Int16 => Value::Int16(reader.read_i16_le().await?),
                Type::Int32 => Value::Int32(reader.read_i32_le().await?),
                Type::Int64 => Value::Int64(reader.read_i64_le().await?),
                Type::Int128 => Value::Int128(reader.read_i128_le().await?),
                Type::Int256 => {
                    let mut buf = [0u8; 32];
                    reader.read_exact(&mut buf[..]).await?;
                    buf.reverse();
                    Value::Int256(i256(buf))
                }
                Type::UInt8 => Value::UInt8(reader.read_u8().await?),
                Type::UInt16 => Value::UInt16(reader.read_u16_le().await?),
                Type::UInt32 => Value::UInt32(reader.read_u32_le().await?),
                Type::UInt64 => Value::UInt64(reader.read_u64_le().await?),
                Type::UInt128 => Value::UInt128(reader.read_u128_le().await?),
                Type::UInt256 => {
                    let mut buf = [0u8; 32];
                    reader.read_exact(&mut buf[..]).await?;
                    buf.reverse();
                    Value::UInt256(u256(buf))
                }
                Type::Float32 => Value::Float32(f32::from_bits(reader.read_u32_le().await?)),
                Type::Float64 => Value::Float64(f64::from_bits(reader.read_u64_le().await?)),
                Type::Decimal32(s) => Value::Decimal32(*s, reader.read_i32_le().await?),
                Type::Decimal64(s) => Value::Decimal64(*s, reader.read_i64_le().await?),
                Type::Decimal128(s) => Value::Decimal128(*s, reader.read_i128_le().await?),
                Type::Decimal256(s) => {
                    let mut buf = [0u8; 32];
                    reader.read_exact(&mut buf[..]).await?;
                    buf.reverse();
                    Value::Decimal256(*s, i256(buf))
                }
                Type::Uuid => Value::Uuid({
                    let n1 = reader.read_u64_le().await?;
                    let n2 = reader.read_u64_le().await?;
                    Uuid::from_u128(((n1 as u128) << 64) | n2 as u128)
                }),
                Type::Date => Value::Date(Date(reader.read_u16_le().await?)),
                Type::DateTime(tz) => Value::DateTime(DateTime(*tz, reader.read_u32_le().await?)),
                Type::Ipv4 => Value::Ipv4(Ipv4Addr::from(reader.read_u32_le().await?).into()),
                Type::Ipv6 => {
                    let mut octets = [0u8; 16];
                    reader.read_exact(&mut octets[..]).await?;
                    Value::Ipv6(Ipv6Addr::from(octets).into())
                }
                Type::DateTime64(precision, tz) => {
                    let raw = reader.read_u64_le().await?;
                    Value::DateTime64(DynDateTime64(*tz, raw, *precision))
                }
                Type::Enum8(_) => Value::Enum8(reader.read_i8().await?),
                Type::Enum16(_) => Value::Enum16(reader.read_i16_le().await?),
                _ => unimplemented!(),
            });
        }
        Ok(out)
    }
}
