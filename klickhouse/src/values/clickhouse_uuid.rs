use crate::{
    convert::{unexpected_type, FromSql},
    types::Type,
    Result, Uuid,
};

use crate::{convert::ToSql, Value};

impl ToSql for Uuid {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
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
