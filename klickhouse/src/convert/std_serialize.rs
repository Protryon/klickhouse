use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap},
};

use indexmap::IndexMap;

use super::*;

impl ToSql for u8 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::UInt8(self))
    }
}

impl ToSql for bool {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::UInt8(self as u8))
    }
}

impl ToSql for u16 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::UInt16(self))
    }
}

impl ToSql for u32 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::UInt32(self))
    }
}

impl ToSql for u64 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::UInt64(self))
    }
}

impl ToSql for u128 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::UInt128(self))
    }
}

impl ToSql for i8 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Int8(self))
    }
}

impl ToSql for i16 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Int16(self))
    }
}

impl ToSql for i32 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Int32(self))
    }
}

impl ToSql for i64 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Int64(self))
    }
}

impl ToSql for i128 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Int128(self))
    }
}

impl ToSql for f32 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Float32(self))
    }
}

impl ToSql for f64 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Float64(self))
    }
}

impl ToSql for String {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::String(self.into_bytes()))
    }
}

impl ToSql for &str {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::String(self.as_bytes().to_vec()))
    }
}

impl<T: ToSql + 'static> ToSql for Vec<T> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        let type_hint = type_hint
            .and_then(|x| x.unarray())
            .map(|x| x.strip_low_cardinality());
        if matches!(type_hint, Some(Type::String) | Some(Type::FixedString(_))) {
            let type_id = TypeId::of::<T>();
            if type_id == TypeId::of::<u8>() || type_id == TypeId::of::<i8>() {
                assert_eq!(std::mem::size_of::<T>(), 1);
                return Ok(Value::String(unsafe {
                    std::mem::transmute::<Vec<T>, Vec<u8>>(self)
                }));
            }
        }
        Ok(Value::Array(
            self.into_iter()
                .map(|x| x.to_sql(type_hint))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl<T: ToSql, Y: ToSql> ToSql for HashMap<T, Y> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.len());
        let mut values = Vec::with_capacity(self.len());
        let type_hint = type_hint.and_then(|x| x.unmap());
        for (key, value) in self {
            keys.push(key.to_sql(type_hint.map(|x| x.0))?);
            values.push(value.to_sql(type_hint.map(|x| x.1))?);
        }
        Ok(Value::Map(keys, values))
    }
}

impl<T: ToSql, Y: ToSql> ToSql for BTreeMap<T, Y> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.len());
        let mut values = Vec::with_capacity(self.len());
        let type_hint = type_hint.and_then(|x| x.unmap());
        for (key, value) in self {
            keys.push(key.to_sql(type_hint.map(|x| x.0))?);
            values.push(value.to_sql(type_hint.map(|x| x.1))?);
        }
        Ok(Value::Map(keys, values))
    }
}

impl<T: ToSql, Y: ToSql> ToSql for IndexMap<T, Y> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.len());
        let mut values = Vec::with_capacity(self.len());
        let type_hint = type_hint.and_then(|x| x.unmap());
        for (key, value) in self {
            keys.push(key.to_sql(type_hint.map(|x| x.0))?);
            values.push(value.to_sql(type_hint.map(|x| x.1))?);
        }
        Ok(Value::Map(keys, values))
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        match self {
            Some(x) => Ok(x.to_sql(type_hint.and_then(|x| x.unnull()))?),
            None => Ok(Value::Null),
        }
    }
}

impl<T: ToSql, const N: usize> ToSql for [T; N] {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        let type_hint = type_hint
            .and_then(|x| x.unarray())
            .map(|x| x.strip_low_cardinality());
        Ok(Value::Array(
            IntoIterator::into_iter(self)
                .map(|x| x.to_sql(type_hint))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl<T: ToSql + Clone> ToSql for &T {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        self.clone().to_sql(type_hint)
    }
}

impl<T: ToSql + Clone> ToSql for &mut T {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        self.clone().to_sql(type_hint)
    }
}

impl<T: ToSql> ToSql for Box<T> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        (*self).to_sql(type_hint)
    }
}

macro_rules! tuple_impls {
    ($($len:expr => ($($n:tt $name:ident)+))+) => {
        $(
            impl<$($name: ToSql),+> ToSql for ($($name,)+) {
                fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
                    let type_hint = type_hint.and_then(|x| x.untuple());

                    Ok(Value::Tuple(vec![
                        $(
                            self.$n.to_sql(type_hint.and_then(|x| x.get($n)))?,
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
