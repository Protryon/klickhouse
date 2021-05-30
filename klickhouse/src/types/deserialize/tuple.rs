use anyhow::*;

use crate::{io::ClickhouseRead, values::Value};

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
            Type::Tuple(inner) => {
                let mut items = Vec::with_capacity(inner.len());
                for item in inner {
                    items.push(item.deserialize(reader, state).await?);
                }
                Value::Tuple(items)
            }
            _ => unimplemented!(),
        })
    }
}
