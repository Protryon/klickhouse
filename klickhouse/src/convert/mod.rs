use std::borrow::Cow;

use crate::{types::Type, KlickhouseError, Result, Value};

mod std_deserialize;
mod std_serialize;

/// A type that can be converted to a raw Clickhouse SQL value.
pub trait ToSql {
    fn to_sql(self) -> Result<Value>;
}

impl ToSql for Value {
    fn to_sql(self) -> Result<Value> {
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
    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self>;

    fn serialize_row(self) -> Result<Vec<(Cow<'static, str>, Value)>>;
}

pub struct UnitValue<T: FromSql + ToSql>(pub T);

impl<T: FromSql + ToSql> Row for UnitValue<T> {
    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        if map.is_empty() {
            return Err(KlickhouseError::MissingField("<unit>"));
        }
        let item = map.into_iter().next().unwrap();
        T::from_sql(item.1, item.2).map(UnitValue)
    }

    fn serialize_row(self) -> Result<Vec<(Cow<'static, str>, Value)>> {
        Ok(vec![(Cow::Borrowed("_"), self.0.to_sql()?)])
    }
}

pub struct RawRow(Vec<(String, Type, Value)>);

impl Row for RawRow {
    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        Ok(Self(
            map.into_iter()
                .map(|(name, type_, value)| (name.to_string(), type_.clone(), value))
                .collect(),
        ))
    }

    fn serialize_row(self) -> Result<Vec<(Cow<'static, str>, Value)>> {
        Ok(self
            .0
            .into_iter()
            .map(|(name, _, value)| (Cow::Owned(name), value))
            .collect())
    }
}
