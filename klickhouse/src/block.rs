use std::{collections::VecDeque, str::FromStr};

use crate::{protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION, Result};
use indexmap::IndexMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    io::{ClickhouseRead, ClickhouseWrite},
    types::{DeserializerState, SerializerState, Type},
    values::Value,
    KlickhouseError,
};

/// Metadata about a block
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub is_overflows: bool,
    pub bucket_num: i32,
}

impl Default for BlockInfo {
    fn default() -> Self {
        BlockInfo {
            is_overflows: false,
            bucket_num: -1,
        }
    }
}

impl BlockInfo {
    async fn read<R: ClickhouseRead>(reader: &mut R) -> Result<Self> {
        let mut new = Self::default();
        loop {
            let field_num = reader.read_var_uint().await?;
            match field_num {
                0 => break,
                1 => {
                    new.is_overflows = reader.read_u8().await? != 0;
                }
                2 => {
                    new.bucket_num = reader.read_i32_le().await?;
                }
                field_num => {
                    return Err(KlickhouseError::ProtocolError(format!(
                        "unknown block info field number: {}",
                        field_num
                    )));
                }
            }
        }
        Ok(new)
    }

    async fn write<W: ClickhouseWrite>(&self, writer: &mut W) -> Result<()> {
        writer.write_var_uint(1).await?;
        writer
            .write_u8(if self.is_overflows { 1 } else { 2 })
            .await?;
        writer.write_var_uint(2).await?;
        writer.write_i32_le(self.bucket_num).await?;
        writer.write_var_uint(0).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
/// A chunk of data in columnar form.
pub struct Block {
    /// Metadata about the block
    pub info: BlockInfo,
    /// The number of rows contained in the block
    pub rows: u64,
    /// The type of each column by name, in order.
    pub column_types: Vec<(String, Type)>,
    /// The data of each column by name, in order. All `Value` should correspond to the associated type in `column_types`.
    pub column_data: Vec<Value>,
}

// Iterator type for `take_iter_rows`
pub struct BlockRowValueIter<'a, I>
where
    I: std::iter::Iterator<Item = Value>,
{
    column_data: Vec<(&'a str, &'a Type, I)>,
}

impl<'a, I> Iterator for BlockRowValueIter<'a, I>
where
    I: Iterator<Item = Value>,
{
    type Item = Vec<(&'a str, &'a Type, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.column_data.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        for (name, type_, pop) in self.column_data.iter_mut() {
            out.push((*name, *type_, pop.next()?));
        }
        Some(out)
    }
}

/// Iterator type for `into_iter_rows`
pub struct BlockRowIntoIter {
    column_data: IndexMap<String, VecDeque<(Type, Value)>>,
}

impl Iterator for BlockRowIntoIter {
    type Item = IndexMap<String, (Type, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out = IndexMap::new();
        if self.column_data.is_empty() {
            return None;
        }
        for (name, value) in self.column_data.iter_mut() {
            out.insert(name.clone(), value.pop_front()?);
        }
        Some(out)
    }
}

impl Block {
    /// Iterate over all rows with owned values.
    pub fn take_iter_rows(&mut self) -> BlockRowValueIter<impl Iterator<Item = Value>> {
        let mut column_data = std::mem::take(&mut self.column_data);
        let mut out = Vec::with_capacity(self.rows as usize);
        for (name, type_) in self.column_types.iter() {
            let mut column = Vec::with_capacity(self.rows as usize);
            let column_slice = column_data.drain(..self.rows as usize);
            column.extend(column_slice);
            out.push((&**name, type_.strip_low_cardinality(), column.into_iter()));
        }
        BlockRowValueIter { column_data: out }
    }

    pub(crate) async fn read<R: ClickhouseRead>(reader: &mut R, revision: u64) -> Result<Self> {
        let info = if revision > 0 {
            BlockInfo::read(reader).await?
        } else {
            Default::default()
        };
        let columns = reader.read_var_uint().await?;
        let rows = reader.read_var_uint().await?;
        let mut block = Block {
            info,
            rows,
            column_types: Vec::with_capacity(columns as usize),
            column_data: Vec::with_capacity(columns as usize),
        };
        for _ in 0..columns {
            let name = reader.read_utf8_string().await?;
            let type_name = reader.read_utf8_string().await?;

            let type_ = Type::from_str(&type_name)?;

            // TODO: implement
            let mut _has_custom_serialization = false;
            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                _has_custom_serialization = reader.read_u8().await? != 0;
            }

            block.column_types.push((name, type_.clone()));

            let mut state = DeserializerState {};
            let mut row_data = if rows > 0 {
                type_.deserialize_prefix(reader, &mut state).await?;
                type_
                    .deserialize_column(reader, rows as usize, &mut state)
                    .await?
            } else {
                vec![]
            };
            block.column_data.append(&mut row_data);
        }

        Ok(block)
    }

    pub(crate) async fn write<W: ClickhouseWrite>(
        mut self,
        writer: &mut W,
        revision: u64,
    ) -> Result<()> {
        if revision > 0 {
            self.info.write(writer).await?;
        }

        let rows = self.rows;

        writer
            .write_var_uint(self.column_types.len() as u64)
            .await?;
        writer.write_var_uint(self.rows).await?;

        for (name, type_) in self.column_types.into_iter() {
            let mut block = Vec::with_capacity(rows as usize);
            block.extend(self.column_data.drain(..rows as usize));

            if block.len() != rows as usize {
                return Err(KlickhouseError::ProtocolError(format!(
                    "row and column length mismatch. {} != {}",
                    block.len(),
                    rows
                )));
            }

            // EncodeStart
            writer.write_string(&name).await?;
            writer.write_string(&type_.to_string()).await?;

            if self.rows > 0 {
                if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                    writer.write_u8(0).await?;
                }

                let mut state = SerializerState {};
                type_.serialize_prefix(writer, &mut state).await?;
                type_.serialize_column(block, writer, &mut state).await?;
            }
        }
        Ok(())
    }
}
