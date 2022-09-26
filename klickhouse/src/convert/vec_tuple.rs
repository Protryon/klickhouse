use crate::{FromSql, KlickhouseError, Result, ToSql, Type, Value};

use super::unexpected_type;

/// A `Vec` wrapper that is encoded as a tuple in SQL as opposed to a Vec
pub struct VecTuple<T>(pub Vec<T>);

impl<T: ToSql> ToSql for VecTuple<T> {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Tuple(
            self.0
                .into_iter()
                .map(|x| x.to_sql())
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl<T: FromSql> FromSql for VecTuple<T> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let subtype = match type_ {
            Type::Tuple(x) => &**x,
            x => return Err(unexpected_type(x)),
        };
        let values = match value {
            Value::Tuple(n) => n,
            _ => unimplemented!(),
        };
        if values.len() != subtype.len() {
            return Err(KlickhouseError::DeserializeError(format!(
                "unexpected type: mismatch tuple length expected {}, got {}",
                subtype.len(),
                values.len()
            )));
        }
        let mut out = Vec::with_capacity(values.len());
        for (type_, value) in subtype.iter().zip(values.into_iter()) {
            out.push(T::from_sql(type_, value)?);
        }
        Ok(VecTuple(out))
    }
}
