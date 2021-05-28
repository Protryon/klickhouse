use anyhow::*;
use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value};

use super::{Deserializer, DeserializerState, Type};


pub struct MapDeserializer;

#[async_trait::async_trait]
impl Deserializer for MapDeserializer {
    async fn read_prefix<R: ClickhouseRead>(type_: &Type, reader: &mut R, state: &mut DeserializerState) -> Result<()> {
        match type_ {
            Type::Map(key, value) => {
                let nested = Type::Array(Box::new(Type::Tuple(vec![(&**key).clone(), (&**value).clone()])));
                nested.deserialize_prefix(reader, state).await?;
            },
            _ => unimplemented!(),
        }
        Ok(())
    }
    
    async fn read<R: ClickhouseRead>(type_: &Type, reader: &mut R, state: &mut DeserializerState) -> Result<Value> {
        Ok(match type_ {
            Type::Map(key, value) => {
                let len = reader.read_u64_le().await?;
                let keys = key.deserialize_column(reader, len as usize, state).await?;
                let values = value.deserialize_column(reader, len as usize, state).await?;
                
                Value::Map(keys, values)
            },
            _ => unimplemented!(),
        })
    }
}