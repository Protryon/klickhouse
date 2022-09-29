use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

pub struct StringSerializer;

#[async_trait::async_trait]
impl Serializer for StringSerializer {
    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        value: Value,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        match value.justify_null_ref(type_).as_ref() {
            Value::String(x) => {
                if let Type::FixedString(s) = type_ {
                    if x.len() >= *s {
                        writer.write_all(&x.as_bytes()[..*s]).await?;
                    } else {
                        writer.write_all(x.as_bytes()).await?;
                        let padding = *s - x.len();
                        for _ in 0..padding {
                            writer.write_u8(0).await?;
                        }
                    }
                } else {
                    writer.write_string(&**x).await?;
                }
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}
