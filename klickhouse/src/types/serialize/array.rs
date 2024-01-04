use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

// Trait to allow serializing [Values] wrapping an array of items.
pub trait ArraySerializerGeneric {
    fn inner_type(type_: &Type) -> &Type;
    fn value_len(value: &Value) -> usize;
    fn values(value: Value) -> Vec<Value>;
}

pub struct ArraySerializer;
impl ArraySerializerGeneric for ArraySerializer {
    fn value_len(value: &Value) -> usize {
        value.unwrap_array_ref().len()
    }
    fn inner_type(type_: &Type) -> &Type {
        type_.unwrap_array()
    }
    fn values(value: Value) -> Vec<Value> {
        value.unwrap_array()
    }
}

#[async_trait::async_trait]
impl<T: ArraySerializerGeneric + 'static> Serializer for T {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        T::inner_type(type_).serialize_prefix(writer, state).await
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let type_ = T::inner_type(type_);
        let mut offset = 0usize;
        for value in &values {
            offset += Self::value_len(value);
            writer.write_u64_le(offset as u64).await?;
        }
        let mut all_values: Vec<Value> = Vec::with_capacity(offset);
        for value in values {
            all_values.extend(Self::values(value));
        }
        type_.serialize_column(all_values, writer, state).await?;
        Ok(())
    }
}
