use tokio::io::AsyncWriteExt;

use crate::{Result, io::ClickhouseWrite, values::Value};

use super::{Serializer, SerializerState, Type};
pub struct NullableSerializer;

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

        let mask = values
            .iter()
            .map(|value| u8::from(value == &Value::Null))
            .collect::<Vec<u8>>();
        writer.write_all(&mask).await?;

        inner_type.serialize_column(values, writer, state).await?;
        Ok(())
    }
}
