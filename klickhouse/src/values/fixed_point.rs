use crate::{
    convert::{unexpected_type, FromSql, ToSql},
    i256,
    types::Type,
    Result, Value,
};

/// Wrapper type for Clickhouse `FixedPoint32` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint32<const PRECISION: u64>(pub i32);

impl<const PRECISION: u64> FixedPoint32<PRECISION> {
    pub const fn modulus(&self) -> i32 {
        10i32.pow(PRECISION as u32)
    }

    pub fn integer(&self) -> i32 {
        self.0 / 10i32.pow(PRECISION as u32)
    }

    pub fn fraction(&self) -> i32 {
        self.0 % 10i32.pow(PRECISION as u32)
    }
}

impl<const PRECISION: u64> ToSql for FixedPoint32<PRECISION> {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Decimal32(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint32<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal32(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal32(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: u64> From<FixedPoint32<PRECISION>> for f64 {
    fn from(fp: FixedPoint32<PRECISION>) -> Self {
        fp.integer() as f64 + (fp.fraction() as f64 / fp.modulus() as f64)
    }
}

/// Wrapper type for Clickhouse `FixedPoint64` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint64<const PRECISION: u64>(pub i64);

impl<const PRECISION: u64> ToSql for FixedPoint64<PRECISION> {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Decimal64(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint64<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal64(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal64(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: u64> FixedPoint64<PRECISION> {
    pub const fn modulus(&self) -> i64 {
        10i64.pow(PRECISION as u32)
    }

    pub fn integer(&self) -> i64 {
        self.0 / 10i64.pow(PRECISION as u32)
    }

    pub fn fraction(&self) -> i64 {
        self.0 % 10i64.pow(PRECISION as u32)
    }
}

impl<const PRECISION: u64> From<FixedPoint64<PRECISION>> for f64 {
    fn from(fp: FixedPoint64<PRECISION>) -> Self {
        fp.integer() as f64 + (fp.fraction() as f64 / fp.modulus() as f64)
    }
}

/// Wrapper type for Clickhouse `FixedPoint128` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint128<const PRECISION: u64>(pub i128);

impl<const PRECISION: u64> ToSql for FixedPoint128<PRECISION> {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Decimal128(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint128<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal128(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal128(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: u64> FixedPoint128<PRECISION> {
    pub const fn modulus(&self) -> i128 {
        10i128.pow(PRECISION as u32)
    }

    pub fn integer(&self) -> i128 {
        self.0 / 10i128.pow(PRECISION as u32)
    }

    pub fn fraction(&self) -> i128 {
        self.0 % 10i128.pow(PRECISION as u32)
    }
}

impl<const PRECISION: u64> From<FixedPoint128<PRECISION>> for f64 {
    fn from(fp: FixedPoint128<PRECISION>) -> Self {
        fp.integer() as f64 + (fp.fraction() as f64 / fp.modulus() as f64)
    }
}

/// Wrapper type for Clickhouse `FixedPoint256` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint256<const PRECISION: u64>(pub i256);

impl<const PRECISION: u64> ToSql for FixedPoint256<PRECISION> {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Decimal256(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint256<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal256(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal256(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}
