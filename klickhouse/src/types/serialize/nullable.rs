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
            log::error!("NullableSerializer called with non-nullable type");
            unimplemented!()
        };

        let mask = values
            .iter()
            .map(|value| u8::from(value == &Value::Null))
            .collect::<Vec<u8>>();
        writer.write_all(&mask).await?;

        inner_type.serialize_column(values, writer, state).await?;
        Ok(())
    }
}
