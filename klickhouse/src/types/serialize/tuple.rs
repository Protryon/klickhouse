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
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_types = if let Type::Tuple(inner_types) = &type_ {
            inner_types
        } else {
            unimplemented!();
        };

        let mut columns = vec![Vec::with_capacity(values.len()); inner_types.len()];

        for value in values {
            let tuple = value.unwrap_tuple();
            for (i, value) in tuple.into_iter().enumerate() {
                columns[i].push(value);
            }
        }
        for (inner_type, column) in inner_types.iter().zip(columns.into_iter()) {
            inner_type.serialize_column(column, writer, state).await?;
        }
        Ok(())
    }
}
