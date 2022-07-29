use anyhow::*;
use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value};

use super::{Deserializer, DeserializerState, Type};

pub struct StringDeserializer;

#[allow(clippy::uninit_vec)]
#[async_trait::async_trait]
impl Deserializer for StringDeserializer {
    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<Value> {
        Ok(match type_ {
            Type::String => Value::String(reader.read_string().await?),
            Type::FixedString(n) => {
                let mut buf = Vec::with_capacity(*n);
                unsafe { buf.set_len(*n) };
                reader.read_exact(&mut buf[..]).await?;
                let first_null = buf.iter().position(|x| *x == 0).unwrap_or(buf.len());
                buf.truncate(first_null);
                Value::String(String::from_utf8(buf)?)
            }
            _ => unimplemented!(),
        })
    }
}
