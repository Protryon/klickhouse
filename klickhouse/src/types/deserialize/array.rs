use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, Result};

use super::{Deserializer, DeserializerState, Type};

/// Trait to allow reading `Item`s and packing them into a `Value::*`.
pub trait ArrayDeserializerGeneric {
    type Item;
    /// The type of the items, e.g. [Value]
    fn inner_type(type_: &Type) -> &Type;
    /// Mapping from items to the return Value, e.g. simply `Vec<Value> -> Value::Array(items)`.
    fn inner_value(items: Vec<Self::Item>) -> Value;
    /// Conversion between the [Value] read and the items, e.g. simply the identity.
    fn item_mapping(value: Value) -> Self::Item;
}

/// Simple case for reading into a [Value::Array].
pub struct ArrayDeserializer;
impl ArrayDeserializerGeneric for ArrayDeserializer {
    type Item = Value;
    fn inner_type(type_: &Type) -> &Type {
        type_.unwrap_array()
    }
    fn inner_value(items: Vec<Self::Item>) -> Value {
        Value::Array(items)
    }
    fn item_mapping(value: Value) -> Value {
        value
    }
}

#[async_trait::async_trait]
impl<T: ArrayDeserializerGeneric + 'static> Deserializer for T {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        Self::inner_type(type_)
            .deserialize_prefix(reader, state)
            .await
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        if rows == 0 {
            return Ok(vec![]);
        }
        let mut offsets = vec![];
        for _ in 0..rows {
            offsets.push(reader.read_u64_le().await?);
        }
        let mut items = Self::inner_type(type_)
            .deserialize_column(reader, offsets[offsets.len() - 1] as usize, state)
            .await?
            .into_iter()
            .map(Self::item_mapping);
        let mut out = Vec::with_capacity(rows);
        let mut read_offset = 0u64;
        for offset in offsets {
            let len = offset - read_offset;
            read_offset = offset;
            out.push(Self::inner_value((&mut items).take(len as usize).collect()));
        }

        Ok(out)
    }
}
