use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

pub struct ArraySerializer;

struct Array2Serializer;

impl Array2Serializer {
    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Array(inner) => {
                let mut offset = 0;
                for value in &values {
                    let inner = value.unwrap_array_ref();
                    offset += inner.len();
                    writer.write_u64_le(offset as u64).await?;
                }
                for value in values {
                    let values = value.unwrap_array();
                    inner.serialize_column(values, writer, state).await?;
                }
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}

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
        value: Value,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match (type_, value.justify_null(type_)) {
            (Type::Array(inner_type), Value::Array(inner)) => {
                writer.write_u64_le(inner.len() as u64).await?;
                if let Type::Array(_) = &**inner_type {
                    return Array2Serializer::write(&**inner_type, inner, writer, state).await;
                }
                inner_type.serialize_column(inner, writer, state).await?;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}
