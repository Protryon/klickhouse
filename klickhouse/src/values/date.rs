use std::num::TryFromIntError;

use chrono::{Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::{Tz, UTC};

use crate::{
    convert::{unexpected_type, FromSql, ToSql},
    types::Type,
    KlickhouseError, Result, Value,
};

/// Wrapper type for Clickhouse `Date` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct Date(pub u16);

impl ToSql for Date {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
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

impl From<Date> for chrono::Date<Utc> {
    fn from(date: Date) -> Self {
        Utc.from_utc_date(&NaiveDate::from_ymd(1970, 01, 01)) + Duration::days(date.0 as i64)
    }
}

impl From<chrono::Date<Utc>> for Date {
    fn from(other: chrono::Date<Utc>) -> Self {
        Self(
            other
                .signed_duration_since(Utc.from_utc_date(&NaiveDate::from_ymd(1970, 01, 01)))
                .num_days() as u16,
        )
    }
}

/// Wrapper type for Clickhouse `DateTime` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime(pub Tz, pub u32);

impl ToSql for DateTime {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
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

impl TryFrom<DateTime> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date.0.timestamp(date.1.try_into()?, 0))
    }
}

impl TryFrom<DateTime> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date.0.timestamp(date.1.try_into()?, 0).with_timezone(&Utc))
    }
}

impl TryFrom<chrono::DateTime<Tz>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Tz>) -> Result<Self, TryFromIntError> {
        Ok(Self(other.timezone(), other.timestamp().try_into()?))
    }
}

impl TryFrom<chrono::DateTime<Utc>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<chrono::Utc>) -> Result<Self, TryFromIntError> {
        Ok(Self(chrono_tz::UTC, other.timestamp().try_into()?))
    }
}

/// Wrapper type for Clickhouse `DateTime64` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime64<const PRECISION: usize>(pub Tz, pub u64);

impl<const PRECISION: usize> ToSql for DateTime64<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
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

impl ToSql for chrono::DateTime<Utc> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(
            chrono_tz::UTC,
            6,
            self.timestamp_micros().try_into().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to convert DateTime64: {:?}", e))
            })?,
        ))
    }
}

impl FromSql for chrono::DateTime<Utc> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(_, _) | Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(tz, precision, value) => {
                let seconds = value / 10u64.pow(precision as u32);
                let units = value % 10u64.pow(precision as u32);
                let units_ns = units * 10u64.pow(9 - precision as u32);
                let (seconds, units_ns): (i64, u32) = seconds
                    .try_into()
                    .and_then(|k| Ok((k, units_ns.try_into()?)))
                    .map_err(|e| {
                        KlickhouseError::DeserializeError(format!(
                            "failed to convert DateTime: {:?}",
                            e
                        ))
                    })?;
                Ok(tz.timestamp(seconds, units_ns).with_timezone(&Utc))
            }
            Value::DateTime(date) => Ok(date.try_into().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to convert DateTime: {:?}", e))
            })?),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        let seconds = date.1 / 10u64.pow(PRECISION as u32);
        let units = date.1 % 10u64.pow(PRECISION as u32);
        let units_ns = units * 10u64.pow(9 - PRECISION as u32);
        Ok(date
            .0
            .timestamp(seconds.try_into()?, units_ns.try_into()?)
            .with_timezone(&Utc))
    }
}

impl ToSql for chrono::DateTime<Tz> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(
            self.timezone(),
            6,
            self.timestamp_micros().try_into().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to convert DateTime64: {:?}", e))
            })?,
        ))
    }
}

impl FromSql for chrono::DateTime<Tz> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(_, _) | Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(tz, precision, value) => {
                let seconds = value / 10u64.pow(precision as u32);
                let units = value % 10u64.pow(precision as u32);
                let units_ns = units * 10u64.pow(9 - precision as u32);
                let (seconds, units_ns): (i64, u32) = seconds
                    .try_into()
                    .and_then(|k| Ok((k, units_ns.try_into()?)))
                    .map_err(|e| {
                        KlickhouseError::DeserializeError(format!(
                            "failed to convert DateTime: {:?}",
                            e
                        ))
                    })?;
                Ok(tz.timestamp(seconds, units_ns))
            }
            Value::DateTime(date) => Ok(date.try_into().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to convert DateTime: {:?}", e))
            })?),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: usize> TryFrom<chrono::DateTime<Utc>> for DateTime64<PRECISION> {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Utc>) -> Result<Self, TryFromIntError> {
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = other.timestamp_subsec_nanos() as u64;
        let total =
            seconds * 10u64.pow(PRECISION as u32) + sub_seconds / 10u64.pow(9 - PRECISION as u32);
        Ok(Self(chrono_tz::UTC, total))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        let seconds = date.1 / 10u64.pow(PRECISION as u32);
        let units = date.1 % 10u64.pow(PRECISION as u32);
        let units_ns = units * 10u64.pow(9 - PRECISION as u32);
        Ok(date.0.timestamp(seconds.try_into()?, units_ns.try_into()?))
    }
}
impl<const PRECISION: usize> TryFrom<chrono::DateTime<Tz>> for DateTime64<PRECISION> {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Tz>) -> Result<Self, TryFromIntError> {
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = other.timestamp_subsec_nanos() as u64;
        let total =
            seconds * 10u64.pow(PRECISION as u32) + sub_seconds / 10u64.pow(9 - PRECISION as u32);
        Ok(Self(other.timezone(), total))
    }
}

#[cfg(test)]
mod chrono_tests {
    use super::*;
    use chrono::TimeZone;
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
            let chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();
            let new_date = DateTime::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64() {
        for i in (0..30000u64).map(|x| x * 10000) {
            let date = DateTime64::<6>(UTC, i);
            let chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();
            let new_date = DateTime64::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64_precision() {
        for i in (0..30000u64).map(|x| x * 10000) {
            let date = DateTime64::<6>(UTC, i);
            let date_value = date.to_sql(None).unwrap();
            assert_eq!(date_value, Value::DateTime64(UTC, 6, i));
            let chrono_date: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(6, UTC), date_value).unwrap();
            let new_date = DateTime64::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64_precision2() {
        for i in (0..300u64).map(|x| x * 1000000) {
            let chrono_time = Utc.timestamp(i as i64, i as u32);
            let date = chrono_time.to_sql(None).unwrap();
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();
            assert_eq!(chrono_time, out_time);
            let date = match date {
                Value::DateTime64(tz, precision, value) => {
                    Value::DateTime64(tz, precision - 3, value / 1000)
                }
                _ => unimplemented!(),
            };
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();

            assert_eq!(chrono_time, out_time);
        }
    }

    #[test]
    fn test_consistency_with_convert_for_str() {
        let test_date = "2022-04-22 00:00:00";

        let dt = chrono::NaiveDateTime::parse_from_str(test_date, "%Y-%m-%d %H:%M:%S").unwrap();

        let chrono_date =
            chrono::DateTime::<Tz>::from_utc(dt, chrono_tz::UTC.offset_from_utc_datetime(&dt));

        let date = DateTime(UTC, dt.timestamp() as u32);

        let new_chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();

        assert_eq!(new_chrono_date, chrono_date);
    }
}
