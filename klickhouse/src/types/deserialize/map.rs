use tokio::io::AsyncReadExt;

use crate::{
    io::ClickhouseRead, protocol::MAX_STRING_SIZE, values::Value, KlickhouseError, Result,
};

use super::{Deserializer, DeserializerState, Type};

pub struct MapDeserializer;

#[async_trait::async_trait]
impl Deserializer for MapDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Map(key, value) => {
                let nested = Type::Array(Box::new(Type::Tuple(vec![
                    (**key).clone(),
                    (**value).clone(),
                ])));
                nested.deserialize_prefix(reader, state).await?;
            }
            _ => {
                return Err(KlickhouseError::DeserializeError(
                    "MapDeserializer called with non-map type".to_string(),
                ))
            }
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        if rows > MAX_STRING_SIZE {
            return Err(KlickhouseError::ProtocolError(format!(
                "read_n response size too large for map. {} > {}",
                rows, MAX_STRING_SIZE
            )));
        }
        if rows == 0 {
            return Ok(vec![]);
        }

        let (key, value) = match type_ {
            Type::Map(key, value) => (key, value),
            _ => {
                return Err(KlickhouseError::DeserializeError(
                    "MapDeserializer called with non-map type".to_string(),
                ))
            }
        };

        let mut offsets: Vec<u64> = Vec::with_capacity(rows);
        for _ in 0..rows {
            offsets.push(reader.read_u64_le().await?);
        }

        let total_length = *offsets.last().unwrap();

        let keys = key
            .deserialize_column(reader, total_length as usize, state)
            .await?;
        assert_eq!(keys.len(), total_length as usize);
        let values = value
            .deserialize_column(reader, total_length as usize, state)
            .await?;
        assert_eq!(values.len(), total_length as usize);

        let mut keys = keys.into_iter();
        let mut values = values.into_iter();
        let mut out = Vec::with_capacity(rows);
        let mut last_offset = 0u64;
        for offset in offsets {
            let mut key_out = vec![];
            let mut value_out = vec![];
            while last_offset < offset {
                key_out.push(keys.next().unwrap());
                value_out.push(values.next().unwrap());
                last_offset += 1;
            }
            out.push(Value::Map(key_out, value_out));
        }
        Ok(out)
    }
}
