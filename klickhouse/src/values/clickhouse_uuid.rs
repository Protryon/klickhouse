use crate::{Uuid, convert::{FromSql, unexpected_type}, types::Type};
use anyhow::*;

use crate::{Value, convert::ToSql};

impl ToSql for Uuid {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Uuid(self))
    }
}

impl FromSql for Uuid {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Uuid) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Uuid(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}