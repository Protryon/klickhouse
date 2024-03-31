use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct NullableDeserializer;

impl Deserializer for NullableDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Nullable(inner) => {
                inner.deserialize_prefix(reader, state).await?;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        // if mask[i] == 0, item is present
        let mut mask = vec![0u8; rows];
        reader.read_exact(&mut mask).await?;

        let mut out = type_
            .strip_null()
            .deserialize_column(reader, rows, state)
            .await?;
        for (i, mask) in mask.iter().enumerate() {
            if *mask != 0 {
                out[i] = Value::Null;
            }
        }

        Ok(out)
    }
}
