use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

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
            match value.justify_null_ref(type_).as_ref() {
                Value::String(bytes) => {
                    emit_bytes(type_, bytes, writer).await?;
                }
                Value::Array(items) => {
                    // validate function already confirmed the types here (it's an indirect Vec<u8>/Vec<i8>)
                    let bytes = items
                        .iter()
                        .map(|x| match x {
                            Value::UInt8(x) => *x,
                            Value::Int8(x) => *x as u8,
                            _ => unimplemented!(),
                        })
                        .collect::<Vec<u8>>();
                    emit_bytes(type_, &bytes, writer).await?;
                }
                _ => unimplemented!(),
            }
        }
        Ok(())
    }
}
