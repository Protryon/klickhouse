use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

pub struct TupleSerializer;

#[async_trait::async_trait]
impl Serializer for TupleSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Tuple(inner) => {
                for item in inner {
                    item.serialize_prefix(writer, state).await?;
                }
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        value: Value,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match (type_, value) {
            (Type::Tuple(types), Value::Tuple(values)) => {
                for (inner_type, value) in types.iter().zip(values.into_iter()) {
                    inner_type.serialize(value, writer, state).await?;
                }
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}
