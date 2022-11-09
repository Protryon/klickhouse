use indexmap::IndexSet;
use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

use crate::types::low_cardinality::*;

pub struct LowCardinalitySerializer;

#[async_trait::async_trait]
impl Serializer for LowCardinalitySerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        writer.write_u64_le(LOW_CARDINALITY_VERSION).await?;
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = match type_ {
            Type::LowCardinality(x) => &**x,
            _ => unimplemented!(),
        };

        if values.is_empty() {
            return Ok(());
        }

        let is_nullable = inner_type.is_nullable();
        let inner_type = inner_type.strip_null();

        let mut keys: IndexSet<&Value> = IndexSet::new();
        let nulled = Value::Null;
        if is_nullable {
            keys.insert(&nulled);
        }
        for value in &values {
            keys.insert(value);
        }

        let mut flags = 0u64;
        if keys.len() > u32::MAX as usize {
            flags |= TUINT64;
        } else if keys.len() > u16::MAX as usize {
            flags |= TUINT32;
        } else if keys.len() > u8::MAX as usize {
            flags |= TUINT16;
        } else {
            flags |= TUINT8
        };
        flags |= HAS_ADDITIONAL_KEYS_BIT;
        writer.write_u64_le(flags).await?;

        writer.write_u64_le(keys.len() as u64).await?;

        inner_type
            .serialize_column(keys.iter().copied().cloned().collect(), writer, state)
            .await?;

        writer.write_u64_le(values.len() as u64).await?;
        for value in &values {
            let index = keys.get_index_of(value).unwrap();
            if keys.len() > u32::MAX as usize {
                writer.write_u64_le(index as u64).await?;
            } else if keys.len() > u16::MAX as usize {
                writer.write_u32_le(index as u32).await?;
            } else if keys.len() > u8::MAX as usize {
                writer.write_u16_le(index as u16).await?;
            } else {
                writer.write_u8(index as u8).await?;
            };
        }
        Ok(())
    }
}
