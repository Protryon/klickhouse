use anyhow::*;
use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value};

use super::{Deserializer, DeserializerState, Type};

pub struct ArrayDeserializer;

struct Array2Deserializer;

impl Array2Deserializer {
    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        n: u64,
        state: &mut DeserializerState,
    ) -> Result<Value> {
        Ok(match type_ {
            Type::Array(inner) => {
                let mut offsets = vec![];
                for _ in 0..n {
                    offsets.push(reader.read_u64_le().await?);
                }
                let mut out = Vec::with_capacity(n as usize);
                let mut read_offset = 0u64;
                for offset in offsets {
                    let len = offset - read_offset;
                    read_offset = offset;
                    let items = inner
                        .deserialize_column(reader, len as usize, state)
                        .await?;
                    out.push(Value::Array(items));
                }

                Value::Array(out)
            }
            _ => unimplemented!(),
        })
    }
}

#[async_trait::async_trait]
impl Deserializer for ArrayDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Array(inner) => {
                inner.deserialize_prefix(reader, state).await?;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<Value> {
        Ok(match type_ {
            Type::Array(inner) => {
                let len = reader.read_u64_le().await?;
                if let Type::Array(_) = &**inner {
                    return Array2Deserializer::read(&**inner, reader, len, state).await;
                }
                let items = inner
                    .deserialize_column(reader, len as usize, state)
                    .await?;
                Value::Array(items)
            }
            _ => unimplemented!(),
        })
    }
}
