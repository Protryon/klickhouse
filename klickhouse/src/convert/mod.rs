use std::borrow::Cow;

use crate::{types::Type, KlickhouseError, Result, Value};

mod raw_row;
mod std_deserialize;
mod std_serialize;
pub use raw_row::*;
mod unit_value;
pub use unit_value::*;
mod vec_tuple;
pub use vec_tuple::*;

/// A type that can be converted to a raw Clickhouse SQL value.
pub trait ToSql {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value>;
}

impl ToSql for Value {
    fn to_sql(self, _type_hint_: Option<&Type>) -> Result<Value> {
        Ok(self)
    }
}

pub fn unexpected_type(type_: &Type) -> KlickhouseError {
    KlickhouseError::DeserializeError(format!("unexpected type: {}", type_))
}

/// A type that can be converted from a raw Clickhouse SQL value.
pub trait FromSql: Sized {
    fn from_sql(type_: &Type, value: Value) -> Result<Self>;
}

impl FromSql for Value {
    fn from_sql(_type_: &Type, value: Value) -> Result<Self> {
        Ok(value)
    }
}

/// A row that can be deserialized and serialized from a raw Clickhouse SQL value.
/// Generally this is not implemented manually, but using `klickhouse_derive::Row`.
/// I.e. `#[derive(klickhouse::Row)]`.
pub trait Row: Sized {
    /// If `Some`, `serialize_row` and `deserialize_row` MUST return this number of columns
    const COLUMN_COUNT: Option<usize>;

    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self>;

    fn serialize_row(self, type_hints: &[&Type]) -> Result<Vec<(Cow<'static, str>, Value)>>;
}
