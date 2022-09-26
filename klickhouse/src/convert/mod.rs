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

/// A row of raw data returned from the database by a query.
#[derive(Debug, Clone)]
pub struct RawRow(Vec<Option<(String, Type, Value)>>);

impl Row for RawRow {
    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        Ok(Self(
            map.into_iter()
                .map(|(name, type_, value)| Some((name.to_string(), type_.clone(), value)))
                .collect(),
        ))
    }

    fn serialize_row(self) -> Result<Vec<(Cow<'static, str>, Value)>> {
        Ok(self
            .0
            .into_iter()
            .map(|x| x.expect("cannot serialize a Row which has been retrieved from"))
            .map(|(name, _, value)| (Cow::Owned(name), value))
            .collect())
    }
}

pub trait RowIndex {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize>;
}

impl RowIndex for usize {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize> {
        if columns.into_iter().count() > *self {
            None
        } else {
            Some(*self)
        }
    }
}

impl RowIndex for str {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize> {
        columns.into_iter().position(|x| x == self)
    }
}

impl<'b, T: RowIndex> RowIndex for &'b T {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize> {
        (*self).get(columns)
    }
}

impl RawRow {
    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Like RawRow::get, but returns a Result rather than panicking.
    pub fn try_get<I: RowIndex, T: FromSql>(&mut self, index: I) -> Result<T> {
        let index = index
            .get(
                self.0
                    .iter()
                    .map(|x| x.as_ref().map(|x| &*x.0).unwrap_or("")),
            )
            .ok_or_else(|| KlickhouseError::OutOfBounds)?;
        let (_, type_, value) = self
            .0
            .get_mut(index)
            .ok_or_else(|| KlickhouseError::OutOfBounds)?
            .take()
            .ok_or_else(|| KlickhouseError::DoubleFetch)?;
        T::from_sql(&type_, value)
    }

    /// Deserializes a value from the row.
    /// The value can be specified either by its numeric index in the row, or by its column name.
    /// # Panics
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<I: RowIndex, T: FromSql>(&mut self, index: I) -> T {
        self.try_get(index).expect("failed to convert column")
    }
}
