use crate::{types::Type, Value};
use anyhow::*;

mod std_deserialize;
mod std_serialize;

pub trait ToSql {
    fn to_sql(self) -> Result<Value>;
}

impl ToSql for Value {
    fn to_sql(self) -> Result<Value> {
        Ok(self)
    }
}

pub fn unexpected_type(type_: &Type) -> anyhow::Error {
    anyhow!("unexpected type: {}", type_.to_string())
}

pub trait FromSql: Sized {
    fn from_sql(type_: &Type, value: Value) -> Result<Self>;
}

impl FromSql for Value {
    fn from_sql(_type_: &Type, value: Value) -> Result<Self> {
        Ok(value)
    }
}

pub trait Row: Sized {
    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self>;

    fn serialize_row(self) -> Result<Vec<(&'static str, Value)>>;
}
