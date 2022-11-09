use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

pub struct ArraySerializer;

#[async_trait::async_trait]
impl Serializer for ArraySerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Array(inner) => {
                inner.serialize_prefix(writer, state).await?;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let type_ = type_.unwrap_array();
        let mut offset = 0usize;
        for value in &values {
            let inner = value.unwrap_array_ref();
            offset += inner.len();
            writer.write_u64_le(offset as u64).await?;
        }
        let mut all_values = Vec::with_capacity(offset);
        for value in values {
            all_values.extend(value.unwrap_array());
        }
        type_.serialize_column(all_values, writer, state).await?;
        Ok(())
    }
}
