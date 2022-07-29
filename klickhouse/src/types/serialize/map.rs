use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

pub struct MapSerializer;

#[async_trait::async_trait]
impl Serializer for MapSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Map(key, value) => {
                let nested = Type::Array(Box::new(Type::Tuple(vec![
                    (&**key).clone(),
                    (&**value).clone(),
                ])));
                nested.serialize_prefix(writer, state).await?;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        value: &Value,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match (type_, value) {
            (Type::Map(key_type, value_type), Value::Map(keys, values)) => {
                writer.write_u64_le(values.len() as u64).await?;
                key_type.serialize_column(&keys[..], writer, state).await?;
                value_type
                    .serialize_column(&values[..], writer, state)
                    .await?;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}
