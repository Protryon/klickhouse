use std::borrow::Cow;

use crate::{FromSql, KlickhouseError, Result, Row, ToSql, Type, Value};

/// A single column row
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
