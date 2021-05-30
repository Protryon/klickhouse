use chrono::{Duration, Utc};
use chrono_tz::{Tz, UTC};

use crate::{
    convert::{unexpected_type, FromSql, ToSql},
    types::Type,
    Value,
};
use anyhow::*;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct Date(pub u16);

impl ToSql for Date {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Date(self))
    }
}

impl FromSql for Date {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Date) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Date(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl Into<chrono::Date<Utc>> for Date {
    fn into(self) -> chrono::Date<Utc> {
        chrono::MIN_DATE + Duration::days(self.0 as i64)
    }
}

impl From<chrono::Date<Utc>> for Date {
    fn from(other: chrono::Date<Utc>) -> Self {
        Self(other.signed_duration_since(chrono::MIN_DATE).num_days() as u16)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime(pub Tz, pub u32);

impl ToSql for DateTime {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::DateTime(self))
    }
}

impl FromSql for DateTime {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl Default for DateTime {
    fn default() -> Self {
        Self(UTC, 0)
    }
}

impl Into<chrono::DateTime<Tz>> for DateTime {
    fn into(self) -> chrono::DateTime<Tz> {
        chrono::MIN_DATETIME.with_timezone(&self.0) + Duration::seconds(self.1 as i64)
    }
}

impl From<chrono::DateTime<Tz>> for DateTime {
    fn from(other: chrono::DateTime<Tz>) -> Self {
        Self(
            other.timezone(),
            other
                .signed_duration_since(chrono::MIN_DATETIME)
                .num_seconds() as u32,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime64<const PRECISION: usize>(pub Tz, pub u64);

impl<const PRECISION: usize> ToSql for DateTime64<PRECISION> {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::DateTime64(self.0, PRECISION, self.1))
    }
}

impl<const PRECISION: usize> FromSql for DateTime64<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(x, _) if *x == PRECISION) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(tz, _, value) => Ok(Self(tz, value)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: usize> Default for DateTime64<PRECISION> {
    fn default() -> Self {
        Self(UTC, 0)
    }
}

impl<const PRECISION: usize> Into<chrono::DateTime<Tz>> for DateTime64<PRECISION> {
    fn into(self) -> chrono::DateTime<Tz> {
        chrono::MIN_DATETIME.with_timezone(&self.0) + Duration::seconds(self.1 as i64)
    }
}

impl<const PRECISION: usize> From<chrono::DateTime<Tz>> for DateTime64<PRECISION> {
    fn from(other: chrono::DateTime<Tz>) -> Self {
        Self(
            other.timezone(),
            other
                .signed_duration_since(chrono::MIN_DATETIME)
                .num_seconds() as u64,
        )
    }
}

#[cfg(test)]
mod chrono_tests {
    use super::*;
    use chrono_tz::UTC;

    #[test]
    fn test_date() {
        for i in 0..30000u16 {
            let date = Date(i);
            let chrono_date: chrono::Date<Utc> = date.into();
            let new_date = Date::from(chrono_date);
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime() {
        for i in (0..30000u32).map(|x| x * 10000) {
            let date = DateTime(UTC, i);
            let chrono_date: chrono::DateTime<Tz> = date.into();
            let new_date = DateTime::from(chrono_date);
            assert_eq!(new_date, date);
        }
    }
}
