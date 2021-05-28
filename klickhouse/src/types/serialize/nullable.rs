use anyhow::*;
use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value};

use super::{Serializer, SerializerState, Type};
pub struct NullableSerializer;

#[async_trait::async_trait]
impl Serializer for NullableSerializer {
    async fn write<W: ClickhouseWrite>(type_: &Type, value: &Value, writer: &mut W, state: &mut SerializerState) -> Result<()> {
        let inner_type = if let Type::Nullable(n) = type_ {
            &**n
        } else {
            unimplemented!()
        };

        match value {
            Value::Null => {
                writer.write_u8(1).await?;
                inner_type.serialize(&inner_type.default_value(), writer, state).await?;
            },
            x => {
                writer.write_u8(0).await?;
                inner_type.serialize(x, writer, state).await?;
            }
        }
        Ok(())
    }

    async fn write_n<W: ClickhouseWrite>(type_: &Type, values: &[Value], writer: &mut W, state: &mut SerializerState) -> Result<()> {
        for value in values {
            let mask = if value == &Value::Null {
                1u8
            } else {
                0u8
            };
            writer.write_u8(mask).await?;
        }
        type_.strip_null().serialize_column(values, writer, state).await?;
        Ok(())
    }
}
