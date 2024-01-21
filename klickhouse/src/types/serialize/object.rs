use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, KlickhouseError, Result};

use super::{Serializer, SerializerState, Type};

pub struct ObjectSerializer;

#[async_trait::async_trait]
impl Serializer for ObjectSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        writer.write_u8(1).await?;
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            let value = if value == Value::Null {
                type_.default_value()
            } else {
                value
            };
            match value {
                Value::Object(bytes) => {
                    writer.write_string(bytes).await?;
                }
                _ => {
                    return Err(KlickhouseError::SerializeError(format!(
                        "ObjectSerializer unimplemented: {type_:?} for value = {value:?}",
                    )));
                }
            }
        }
        Ok(())
    }
}
