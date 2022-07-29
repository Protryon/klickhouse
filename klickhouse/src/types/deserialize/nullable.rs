use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct NullableDeserializer;

#[async_trait::async_trait]
impl Deserializer for NullableDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Nullable(inner) => {
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
            Type::Nullable(inner) => {
                let is_present = reader.read_u8().await? == 0;
                //todo: eat bytes but discard better
                let value = inner.deserialize(reader, state).await?;
                if is_present {
                    value
                } else {
                    Value::Null
                }
            }
            _ => unimplemented!(),
        })
    }

    async fn read_n<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        n: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut mask = vec![false; n];
        #[allow(clippy::needless_range_loop)]
        for i in 0..n {
            let octet = reader.read_u8().await?;
            if octet == 0 {
                mask[i] = true;
            }
        }
        let mut out = type_
            .strip_null()
            .deserialize_column(reader, n, state)
            .await?;
        for (i, mask) in mask.iter().enumerate() {
            if !*mask {
                out[i] = Value::Null;
            }
        }

        Ok(out)
    }
}
