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

    async fn write_n<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let (key_type, value_type) = match type_ {
            Type::Map(key, value) => (key, value),
            _ => unimplemented!(),
        };

        let mut total_keys = vec![];
        let mut total_values = vec![];

        for value in values {
            let (keys, values) = match value {
                Value::Map(keys, values) => (keys, values),
                _ => unimplemented!(),
            };
            assert_eq!(keys.len(), values.len());
            writer
                .write_u64_le((total_keys.len() + keys.len()) as u64)
                .await?;
            total_keys.extend(keys);
            total_values.extend(values);
        }

        key_type.serialize_column(total_keys, writer, state).await?;
        value_type
            .serialize_column(total_values, writer, state)
            .await?;
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        value: Value,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        Self::write_n(type_, vec![value], writer, state).await
    }
}
