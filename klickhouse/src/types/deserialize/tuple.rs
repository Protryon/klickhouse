use crate::{io::ClickhouseRead, values::Value, KlickhouseError, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct TupleDeserializer;

#[async_trait::async_trait]
impl Deserializer for TupleDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Tuple(inner) => {
                for item in inner {
                    item.deserialize_prefix(reader, state).await?;
                }
            }
            _ => {
                return Err(KlickhouseError::DeserializeError(
                    "TupleDeserializer called with non-tuple type".to_string(),
                ))
            }
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let types = type_.unwrap_tuple()?;
        let mut tuples = vec![Value::Tuple(Vec::with_capacity(types.len())); rows];
        for type_ in types {
            for (i, value) in type_
                .deserialize_column(reader, rows, state)
                .await?
                .into_iter()
                .enumerate()
            {
                match &mut tuples[i] {
                    Value::Tuple(values) => {
                        values.push(value);
                    }
                    _ => {
                        return Err(KlickhouseError::DeserializeError(
                            "Expected tuple".to_string(),
                        ))
                    }
                }
            }
        }
        Ok(tuples)
    }
}
