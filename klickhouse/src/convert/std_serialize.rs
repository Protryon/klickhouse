use std::collections::{BTreeMap, HashMap};

use indexmap::IndexMap;

use super::*;

impl ToSql for u8 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::UInt8(self))
    }
}

impl ToSql for u16 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::UInt16(self))
    }
}

impl ToSql for u32 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::UInt32(self))
    }
}

impl ToSql for u64 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::UInt64(self))
    }
}

impl ToSql for u128 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::UInt128(self))
    }
}

impl ToSql for i8 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Int8(self))
    }
}

impl ToSql for i16 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Int16(self))
    }
}

impl ToSql for i32 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Int32(self))
    }
}

impl ToSql for i64 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Int64(self))
    }
}

impl ToSql for i128 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Int128(self))
    }
}

impl ToSql for f32 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Float32(self.to_bits()))
    }
}

impl ToSql for f64 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Float64(self.to_bits()))
    }
}

impl ToSql for String {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::String(self))
    }
}

impl<'a> ToSql for &'a str {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::String(self.to_string()))
    }
}

impl<T: ToSql> ToSql for Vec<T> {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Array(
            self.into_iter()
                .map(|x| x.to_sql())
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl<T: ToSql, Y: ToSql> ToSql for HashMap<T, Y> {
    fn to_sql(self) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.len());
        let mut values = Vec::with_capacity(self.len());
        for (key, value) in self {
            keys.push(key.to_sql()?);
            values.push(value.to_sql()?);
        }
        Ok(Value::Map(keys, values))
    }
}

impl<T: ToSql, Y: ToSql> ToSql for BTreeMap<T, Y> {
    fn to_sql(self) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.len());
        let mut values = Vec::with_capacity(self.len());
        for (key, value) in self {
            keys.push(key.to_sql()?);
            values.push(value.to_sql()?);
        }
        Ok(Value::Map(keys, values))
    }
}

impl<T: ToSql, Y: ToSql> ToSql for IndexMap<T, Y> {
    fn to_sql(self) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.len());
        let mut values = Vec::with_capacity(self.len());
        for (key, value) in self {
            keys.push(key.to_sql()?);
            values.push(value.to_sql()?);
        }
        Ok(Value::Map(keys, values))
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(self) -> Result<Value> {
        match self {
            Some(x) => Ok(x.to_sql()?),
            None => Ok(Value::Null),
        }
    }
}

#[cfg(const_generics)]
impl<T: ToSql, const N: usize> ToSql for [T; N] {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Array(
            IntoIterator::into_iter(self)
                .map(|x| x.to_sql())
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl<'a, T: ToSql + Clone> ToSql for &'a T {
    fn to_sql(self) -> Result<Value> {
        self.clone().to_sql()
    }
}

impl<'a, T: ToSql + Clone> ToSql for &'a mut T {
    fn to_sql(self) -> Result<Value> {
        self.clone().to_sql()
    }
}

impl<T: ToSql> ToSql for Box<T> {
    fn to_sql(self) -> Result<Value> {
        (*self).to_sql()
    }
}

macro_rules! tuple_impls {
    ($($len:expr => ($($n:tt $name:ident)+))+) => {
        $(
            impl<$($name: ToSql),+> ToSql for ($($name,)+) {
                fn to_sql(self) -> Result<Value> {
                    Ok(Value::Tuple(vec![
                        $(
                            self.$n.to_sql()?,
                        )+
                    ]))
                }
            }
        )+
    }
}

tuple_impls! {
    1 => (0 T0)
    2 => (0 T0 1 T1)
    3 => (0 T0 1 T1 2 T2)
    4 => (0 T0 1 T1 2 T2 3 T3)
    5 => (0 T0 1 T1 2 T2 3 T3 4 T4)
    6 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5)
    7 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6)
    8 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7)
    9 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8)
    10 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9)
    11 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10)
    12 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11)
    13 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12)
    14 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12 13 T13)
    15 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12 13 T13 14 T14)
    16 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11 12 T12 13 T13 14 T14 15 T15)
}
