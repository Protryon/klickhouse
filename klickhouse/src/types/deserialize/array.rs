use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct ArrayDeserializer;

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
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let type_ = type_.unwrap_array();
        if rows == 0 {
            return Ok(vec![]);
        }
        let mut offsets = vec![];
        for _ in 0..rows {
            offsets.push(reader.read_u64_le().await?);
        }
        let mut items = type_
            .deserialize_column(reader, offsets[offsets.len() - 1] as usize, state)
            .await?
            .into_iter();
        let mut out = Vec::with_capacity(rows as usize);
        let mut read_offset = 0u64;
        for offset in offsets {
            let len = offset - read_offset;
            read_offset = offset;
            out.push(Value::Array((&mut items).take(len as usize).collect()));
        }

        Ok(out)
    }
}
