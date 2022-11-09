use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};
pub struct NullableSerializer;

#[async_trait::async_trait]
impl Serializer for NullableSerializer {
    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = if let Type::Nullable(n) = type_ {
            &**n
        } else {
            unimplemented!()
        };

        for value in &values {
            let mask = if value == &Value::Null { 1u8 } else { 0u8 };
            writer.write_u8(mask).await?;
        }

        inner_type.serialize_column(values, writer, state).await?;
        Ok(())
    }
}
