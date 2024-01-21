use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, KlickhouseError, Result};

use super::{Serializer, SerializerState, Type};

pub struct StringSerializer;

async fn emit_bytes<W: ClickhouseWrite>(type_: &Type, bytes: &[u8], writer: &mut W) -> Result<()> {
    if let Type::FixedString(s) = type_ {
        if bytes.len() >= *s {
            writer.write_all(&bytes[..*s]).await?;
        } else {
            writer.write_all(bytes).await?;
            let padding = *s - bytes.len();
            for _ in 0..padding {
                writer.write_u8(0).await?;
            }
        }
    } else {
        writer.write_string(bytes).await?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl Serializer for StringSerializer {
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
                Value::String(bytes) => {
                    emit_bytes(type_, &bytes, writer).await?;
                }
                Value::Array(items) => {
                    // validate function already confirmed the types here (it's an indirect Vec<u8>/Vec<i8>)
                    let bytes = items
                        .into_iter()
                        .filter_map(|x| {
                            match x {
                                Value::UInt8(x) => Ok(x),
                                Value::Int8(x) => Ok(x as u8),
                                // TODO: This is wrong, it will never deserialize w/ missing pieces
                                _ => Err(KlickhouseError::SerializeError(format!(
                                    "StringSerializer called with non-string type: {:?}",
                                    type_
                                ))),
                            }
                            .ok()
                        })
                        .collect::<Vec<u8>>();
                    emit_bytes(type_, &bytes, writer).await?;
                }
                _ => {
                    return Err(KlickhouseError::SerializeError(format!(
                        "StringSerializer unimplemented: {type_:?} for value = {value:?}",
                    )));
                }
            }
        }
        Ok(())
    }
}
