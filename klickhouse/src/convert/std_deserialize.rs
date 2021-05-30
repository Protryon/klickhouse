use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

use indexmap::IndexMap;

use super::*;

impl FromSql for u8 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::UInt8) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::UInt8(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for u16 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::UInt16) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::UInt16(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for u32 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::UInt32) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::UInt32(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for u64 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::UInt64) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::UInt64(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for u128 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::UInt128) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::UInt128(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for i8 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Int8) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Int8(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for i16 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Int16) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Int16(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for i32 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Int32) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Int32(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for i64 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Int64) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Int64(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for i128 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Int128) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Int128(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for f32 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Float32) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Float32(x) => Ok(f32::from_bits(x)),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for f64 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Float64) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Float64(x) => Ok(f64::from_bits(x)),
            _ => unimplemented!(),
        }
    }
}

impl FromSql for String {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::String | Type::FixedString(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::String(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl<T: FromSql> FromSql for Vec<T> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let subtype = match type_ {
            Type::Array(x) => &**x,
            x => return Err(unexpected_type(x)),
        }
        .strip_low_cardinality();
        match value {
            Value::Array(x) => Ok(x
                .into_iter()
                .map(|x| T::from_sql(subtype, x))
                .collect::<Result<Vec<_>>>()?),
            _ => unimplemented!(),
        }
    }
}

impl<T: FromSql + Hash + Eq, Y: FromSql> FromSql for HashMap<T, Y> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let (x_type, y_type) = match type_ {
            Type::Map(x_type, y_type) => (
                x_type.strip_low_cardinality(),
                y_type.strip_low_cardinality(),
            ),
            x => return Err(unexpected_type(x)),
        };
        match value {
            Value::Map(x, y) => {
                let mut out = HashMap::new();
                for (x, y) in x.into_iter().zip(y.into_iter()) {
                    out.insert(T::from_sql(x_type, x)?, Y::from_sql(y_type, y)?);
                }
                Ok(out)
            }
            _ => unimplemented!(),
        }
    }
}

impl<T: FromSql + Ord, Y: FromSql> FromSql for BTreeMap<T, Y> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let (x_type, y_type) = match type_ {
            Type::Map(x_type, y_type) => (
                x_type.strip_low_cardinality(),
                y_type.strip_low_cardinality(),
            ),
            x => return Err(unexpected_type(x)),
        };
        match value {
            Value::Map(x, y) => {
                let mut out = BTreeMap::new();
                for (x, y) in x.into_iter().zip(y.into_iter()) {
                    out.insert(T::from_sql(x_type, x)?, Y::from_sql(y_type, y)?);
                }
                Ok(out)
            }
            _ => unimplemented!(),
        }
    }
}

impl<T: FromSql + Hash + Eq, Y: FromSql> FromSql for IndexMap<T, Y> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let (x_type, y_type) = match type_ {
            Type::Map(x_type, y_type) => (
                x_type.strip_low_cardinality(),
                y_type.strip_low_cardinality(),
            ),
            x => return Err(unexpected_type(x)),
        };
        match value {
            Value::Map(x, y) => {
                let mut out = IndexMap::new();
                for (x, y) in x.into_iter().zip(y.into_iter()) {
                    out.insert(T::from_sql(x_type, x)?, Y::from_sql(y_type, y)?);
                }
                Ok(out)
            }
            _ => unimplemented!(),
        }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let subtype = match type_ {
            Type::Nullable(x) => x.strip_low_cardinality(),
            x => return Err(unexpected_type(x)),
        };
        match value {
            Value::Null => Ok(None),
            x => Ok(Some(T::from_sql(subtype, x)?)),
        }
    }
}

#[cfg(const_generics)]
impl<T: FromSql + Default + Copy, const N: usize> FromSql for [T; N] {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let subtype = match type_ {
            Type::Array(x) => x.strip_low_cardinality(),
            x => return Err(unexpected_type(x)),
        };
        match value {
            Value::Array(x) => {
                if x.len() != N {
                    return Err(anyhow!(
                        "invalid length for array: {} expected {}",
                        x.len(),
                        N
                    ));
                }
                let mut out = [T::default(); N];
                for (i, value) in x.into_iter().enumerate() {
                    out[i] = T::from_sql(subtype, value)?;
                }
                Ok(out)
            }
            _ => unimplemented!(),
        }
    }
}

impl<T: FromSql> FromSql for Box<T> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        Ok(Box::new(T::from_sql(type_, value)?))
    }
}

macro_rules! tuple_impls {
    ($($len:expr => ($($n:tt $name:ident)+))+) => {
        $(
            impl<$($name: FromSql),+> FromSql for ($($name,)+) {
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
                        return Err(anyhow!("mismatch tuple length {} vs {}", values.len(), subtype.len()));
                    }
                    if values.len() != $len {
                        return Err(anyhow!("unexpected tuple length, got {} expecting {}", values.len(), $len));
                    }
                    let mut deque = std::collections::VecDeque::from(values);
                    Ok((
                        $(
                            $name::from_sql(subtype[$n].strip_low_cardinality(), deque.pop_front().unwrap())?,
                        )+
                    ))
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
