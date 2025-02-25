use std::borrow::Cow;

use crate::{FromSql, KlickhouseError, Result, Row, ToSql, Type, Value};

/// A row of raw data returned from the database by a query.
/// Or an unstructured runtime-defined row to upload to the server.
#[derive(Debug, Default, Clone)]
pub struct RawRow(Vec<Option<(String, Type, Value)>>);

impl Row for RawRow {
    const COLUMN_COUNT: Option<usize> = None;

    fn column_names() -> Option<Vec<Cow<'static, str>>> {
        None
    }

    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        Ok(Self(
            map.into_iter()
                .map(|(name, type_, value)| Some((name.to_string(), type_.clone(), value)))
                .collect(),
        ))
    }

    fn serialize_row(
        self,
        _type_hints: &indexmap::IndexMap<String, Type>,
    ) -> Result<Vec<(Cow<'static, str>, Value)>> {
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
        let count = columns.into_iter().count();
        if count >= *self {
            Some(*self)
        } else {
            None
        }
    }
}

impl RowIndex for str {
    fn get<'a, I: IntoIterator<Item = &'a str>>(&self, columns: I) -> Option<usize> {
        columns.into_iter().position(|x| x == self)
    }
}

impl<T: RowIndex + ?Sized> RowIndex for &T {
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
            .ok_or(KlickhouseError::OutOfBounds)?;
        let (_, type_, value) = self
            .0
            .get_mut(index)
            .unwrap()
            .take()
            .ok_or(KlickhouseError::DoubleFetch)?;
        T::from_sql(&type_, value)
    }

    /// Deserializes a value from the row.
    /// The value can be specified either by its numeric index in the row, or by its column name.
    /// # Panics
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<I: RowIndex, T: FromSql>(&mut self, index: I) -> T {
        self.try_get(index).expect("failed to convert column")
    }

    /// Sets or inserts a column value with a given name. `type_` is inferred if `None`. Index is defined on insertion order.
    pub fn try_set_typed(
        &mut self,
        name: impl ToString,
        type_: Option<Type>,
        value: impl ToSql,
    ) -> Result<()> {
        let name = name.to_string();
        let value = value.to_sql(type_.as_ref())?;
        let type_ = type_.unwrap_or_else(|| value.guess_type());

        let current_position = self
            .0
            .iter()
            .map(|x| x.as_ref().map(|x| &*x.0).unwrap_or(""))
            .position(|x| x == &*name);

        if let Some(current_position) = current_position {
            self.0[current_position].as_mut().unwrap().1 = type_;
            self.0[current_position].as_mut().unwrap().2 = value;
        } else {
            self.0.push(Some((name, type_, value)));
        }
        Ok(())
    }

    /// Same as `try_set_typed`, but always infers the type
    pub fn try_set(&mut self, name: impl ToString, value: impl ToSql) -> Result<()> {
        self.try_set_typed(name, None, value)
    }

    /// Same as `try_set`, but panics on type conversion failure.
    pub fn set(&mut self, name: impl ToString, value: impl ToSql) {
        self.try_set(name, value).expect("failed to convert column");
    }

    /// Same as `try_set_typed`, but panics on type conversion failure.
    pub fn set_typed(&mut self, name: impl ToString, type_: Option<Type>, value: impl ToSql) {
        self.try_set_typed(name, type_, value)
            .expect("failed to convert column");
    }
}
