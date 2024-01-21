use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, KlickhouseError, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct ObjectDeserializer;

#[allow(clippy::uninit_vec)]
#[async_trait::async_trait]
impl Deserializer for ObjectDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Object => reader.read_i8().await?,
            _ => {
                return Err(KlickhouseError::DeserializeError(
                    "ObjectDeserializer called with non-json type".to_string(),
                ))
            }
        };
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        match type_ {
            Type::Object => {
                let mut out = Vec::with_capacity(rows);
                for _ in 0..rows {
                    out.push(Value::String(reader.read_string().await?));
                }
                Ok(out)
            }
            _ => Err(crate::KlickhouseError::DeserializeError(
                "ObjectDeserializer called with non-json type".to_string(),
            )),
        }
    }
}
