use std::{collections::VecDeque, str::FromStr};

use anyhow::*;
use indexmap::IndexMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    io::{ClickhouseRead, ClickhouseWrite},
    types::{DeserializerState, SerializerState, Type},
    values::Value,
};

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
    pub async fn read<R: ClickhouseRead>(reader: &mut R) -> Result<Self> {
        let mut new = Self::default();
        loop {
            let field_num = reader.read_var_uint().await?;
            match field_num {
                0 => break,
                1 => {
                    new.is_overflows = reader.read_u8().await? != 0;
                }
                2 => {
                    new.bucket_num = reader.read_i32().await?;
                }
                field_num => {
                    return Err(anyhow!("unknown block info field number: {}", field_num));
                }
            }
        }
        Ok(new)
    }

    pub async fn write<W: ClickhouseWrite>(&self, writer: &mut W) -> Result<()> {
        writer.write_var_uint(1).await?;
        writer
            .write_u8(if self.is_overflows { 1 } else { 2 })
            .await?;
        writer.write_var_uint(2).await?;
        writer.write_i32(self.bucket_num).await?;
        writer.write_var_uint(0).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Block {
    pub info: BlockInfo,
    pub rows: u64,
    pub column_types: IndexMap<String, Type>,
    pub column_data: IndexMap<String, Vec<Value>>,
}

pub struct BlockRowIter<'a> {
    block: &'a Block,
    row: u64,
}

impl<'a> Iterator for BlockRowIter<'a> {
    type Item = Vec<(&'a str, &'a Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.block.rows {
            return None;
        }
        let mut out = Vec::new();
        for (name, value) in self.block.column_data.iter() {
            out.push((&**name, value.get(self.row as usize)?));
        }
        self.row += 1;
        Some(out)
    }
}
pub struct BlockRowValueIter<'a> {
    column_data: Vec<(&'a str, &'a Type, std::vec::IntoIter<Value>)>,
}

impl<'a> Iterator for BlockRowValueIter<'a> {
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

pub struct BlockRowIntoIter {
    column_data: IndexMap<String, VecDeque<(Type, Value)>>,
}

impl Iterator for BlockRowIntoIter {
    type Item = IndexMap<String, (Type, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out = IndexMap::new();
        for (name, value) in self.column_data.iter_mut() {
            out.insert(name.clone(), value.pop_front()?);
        }
        Some(out)
    }
}

impl Block {
    pub fn iter_rows(&self) -> BlockRowIter<'_> {
        BlockRowIter {
            block: self,
            row: 0,
        }
    }

    pub fn take_iter_rows(&mut self) -> BlockRowValueIter {
        let mut column_data = IndexMap::new();
        std::mem::swap(&mut self.column_data, &mut column_data);
        let mut out = Vec::with_capacity(self.rows as usize);
        for (name, values) in column_data.into_iter() {
            let (name, type_) = self.column_types.get_key_value(&name).unwrap();
            out.push((&**name, type_.strip_low_cardinality(), values.into_iter()));
        }
        BlockRowValueIter { column_data: out }
    }

    pub fn into_iter_rows(self) -> BlockRowIntoIter {
        let column_types = self.column_types;
        BlockRowIntoIter {
            column_data: self
                .column_data
                .into_iter()
                .map(|(name, values)| {
                    let type_ = column_types.get(&name).unwrap();
                    (
                        name,
                        values.into_iter().map(|x| (type_.clone(), x)).collect(),
                    )
                })
                .collect(),
        }
    }

    pub async fn read<R: ClickhouseRead>(reader: &mut R, revision: u64) -> Result<Self> {
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
            column_types: IndexMap::new(),
            column_data: IndexMap::new(),
        };
        for _ in 0..columns {
            let name = reader.read_string().await?;
            let type_name = reader.read_string().await?;
            let type_ = Type::from_str(&*type_name)?;
            block.column_types.insert(name.clone(), type_.clone());
            let mut state = DeserializerState {};
            let row_data = if rows > 0 {
                type_.deserialize_prefix(reader, &mut state).await?;
                type_
                    .deserialize_column(reader, rows as usize, &mut state)
                    .await?
            } else {
                vec![]
            };
            block.column_data.insert(name, row_data);
        }

        Ok(block)
    }

    pub async fn write<W: ClickhouseWrite>(&self, writer: &mut W, revision: u64) -> Result<()> {
        if revision > 0 {
            self.info.write(writer).await?;
        }
        let joined = self
            .column_types
            .iter()
            .flat_map(|(key, type_)| Some((key, (type_, self.column_data.get(key)?))))
            .collect::<Vec<_>>();
        writer.write_var_uint(joined.len() as u64).await?;
        writer.write_var_uint(self.rows).await?;
        for (name, (type_, data)) in joined {
            writer.write_string(&*name).await?;
            writer.write_string(&*type_.to_string()).await?;
            if data.len() != self.rows as usize {
                return Err(anyhow!("row and column length mismatch"));
            }
            if self.rows > 0 {
                let mut state = SerializerState {};
                type_.serialize_prefix(writer, &mut state).await?;
                type_
                    .serialize_column(&data[..], writer, &mut state)
                    .await?;
            }
        }
        Ok(())
    }
}
