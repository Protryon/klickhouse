use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{FromSql, KlickhouseError, Result, ToSql, Type, Value};

/// A `Vec` wrapper that is encoded as a tuple in SQL as opposed to a Vec
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Json<T>(pub T);

impl<T: Serialize> ToSql for Json<T> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::String(
            serde_json::to_string(&self.0)
                .map_err(|e| KlickhouseError::SerializeError(e.to_string()))?
                .into_bytes(),
        ))
    }
}

impl<T: DeserializeOwned> FromSql for Json<T> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let raw: String = FromSql::from_sql(type_, value)?;

        Ok(Json(serde_json::from_str(&raw).map_err(|e| {
            KlickhouseError::DeserializeError(e.to_string())
        })?))
    }
}
