use std::num::TryFromIntError;

use chrono::{Duration, FixedOffset, NaiveDate, ParseError, TimeZone, Utc};
use chrono_tz::{Tz, UTC};

use crate::{
    convert::{unexpected_type, FromSql, ToSql},
    types::Type,
    KlickhouseError, Result, Value,
};

/// Wrapper type for Clickhouse `Date` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct Date(pub u16);

#[cfg(feature = "serde")]
impl serde::Serialize for Date {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: NaiveDate = (*self).into();
        date.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Date {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let date: NaiveDate = NaiveDate::deserialize(deserializer)?;
        Ok(date.into())
    }
}

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

#[allow(deprecated)]
impl From<Date> for chrono::Date<Utc> {
    fn from(date: Date) -> Self {
        Utc.from_utc_date(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            + Duration::days(date.0 as i64)
    }
}

#[allow(deprecated)]
impl From<chrono::Date<Utc>> for Date {
    fn from(other: chrono::Date<Utc>) -> Self {
        Self(
            other
                .signed_duration_since(
                    Utc.from_utc_date(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
                )
                .num_days() as u16,
        )
    }
}

impl From<Date> for chrono::NaiveDate {
    fn from(date: Date) -> Self {
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(date.0 as i64)
    }
}

impl From<chrono::NaiveDate> for Date {
    fn from(other: chrono::NaiveDate) -> Self {
        Self(
            other
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as u16,
        )
    }
}

/// Wrapper type for Clickhouse `DateTime` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime(pub Tz, pub u32);

#[cfg(feature = "serde")]
impl serde::Serialize for DateTime {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for DateTime {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<FixedOffset> =
            chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: ParseError| serde::de::Error::custom(e.to_string()))?;

        date.try_into()
            .map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

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
        Ok(date.0.timestamp_opt(date.1.into(), 0).unwrap())
    }
}

impl TryFrom<DateTime> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date
            .0
            .timestamp_opt(date.1.into(), 0)
            .unwrap()
            .fixed_offset())
    }
}

impl TryFrom<DateTime> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date
            .0
            .timestamp_opt(date.1.into(), 0)
            .unwrap()
            .with_timezone(&Utc))
    }
}

impl TryFrom<chrono::DateTime<Tz>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Tz>) -> Result<Self, TryFromIntError> {
        Ok(Self(other.timezone(), other.timestamp().try_into()?))
    }
}

impl TryFrom<chrono::DateTime<FixedOffset>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<FixedOffset>) -> Result<Self, TryFromIntError> {
        chrono_tz::Tz::UTC
            .from_utc_datetime(&other.naive_utc())
            .try_into()
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

/// Wrapper type for Clickhouse `DateTime64` type with dynamic precision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DynDateTime64(pub Tz, pub u64, pub usize);

impl<const PRECISION: usize> From<DateTime64<PRECISION>> for DynDateTime64 {
    fn from(value: DateTime64<PRECISION>) -> Self {
        Self(value.0, value.1, PRECISION)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for DynDateTime64 {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for DynDateTime64 {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<Utc> = Utc.from_utc_datetime(
            &chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: ParseError| serde::de::Error::custom(e.to_string()))?
                .naive_utc(),
        );

        DynDateTime64::try_from_utc(date, 6)
            .map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

#[cfg(feature = "serde")]
impl<const PRECISION: usize> serde::Serialize for DateTime64<PRECISION> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, const PRECISION: usize> serde::Deserialize<'de> for DateTime64<PRECISION> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<Utc> = Utc.from_utc_datetime(
            &chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: ParseError| serde::de::Error::custom(e.to_string()))?
                .naive_utc(),
        );

        date.try_into()
            .map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

impl<const PRECISION: usize> ToSql for DateTime64<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(self.into()))
    }
}

impl<const PRECISION: usize> FromSql for DateTime64<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(x, _) if *x == PRECISION) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => Ok(Self(datetime.0, datetime.1)),
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
        Ok(Value::DateTime64(DynDateTime64(
            chrono_tz::UTC,
            self.timestamp_micros().try_into().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to convert DateTime64: {:?}", e))
            })?,
            6,
        )))
    }
}

impl FromSql for chrono::DateTime<Utc> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(_, _) | Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => {
                let seconds = datetime.1 / 10u64.pow(datetime.2 as u32);
                let units = datetime.1 % 10u64.pow(datetime.2 as u32);
                let units_ns = units * 10u64.pow(9 - datetime.2 as u32);
                let (seconds, units_ns): (i64, u32) = seconds
                    .try_into()
                    .and_then(|k| Ok((k, units_ns.try_into()?)))
                    .map_err(|e| {
                        KlickhouseError::DeserializeError(format!(
                            "failed to convert DateTime: {:?}",
                            e
                        ))
                    })?;
                Ok(datetime
                    .0
                    .timestamp_opt(seconds, units_ns)
                    .unwrap()
                    .with_timezone(&Utc))
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
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap()
            .with_timezone(&Utc))
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        let seconds = date.1 / 10u64.pow(date.2 as u32);
        let units = date.1 % 10u64.pow(date.2 as u32);
        let units_ns = units * 10u64.pow(9 - date.2 as u32);
        Ok(date
            .0
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap()
            .with_timezone(&Utc))
    }
}

impl ToSql for chrono::DateTime<Tz> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(DynDateTime64(
            self.timezone(),
            self.timestamp_micros().try_into().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to convert DateTime64: {:?}", e))
            })?,
            6,
        )))
    }
}

impl FromSql for chrono::DateTime<Tz> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(_, _) | Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => {
                let seconds = datetime.1 / 10u64.pow(datetime.2 as u32);
                let units = datetime.1 % 10u64.pow(datetime.2 as u32);
                let units_ns = units * 10u64.pow(9 - datetime.2 as u32);
                let (seconds, units_ns): (i64, u32) = seconds
                    .try_into()
                    .and_then(|k| Ok((k, units_ns.try_into()?)))
                    .map_err(|e| {
                        KlickhouseError::DeserializeError(format!(
                            "failed to convert DateTime: {:?}",
                            e
                        ))
                    })?;
                Ok(datetime.0.timestamp_opt(seconds, units_ns).unwrap())
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

impl DynDateTime64 {
    pub fn try_from_utc(
        other: chrono::DateTime<Utc>,
        precision: usize,
    ) -> Result<Self, TryFromIntError> {
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = other.timestamp_subsec_nanos() as u64;
        let total =
            seconds * 10u64.pow(precision as u32) + sub_seconds / 10u64.pow(9 - precision as u32);
        Ok(Self(chrono_tz::UTC, total, precision))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        let seconds = date.1 / 10u64.pow(PRECISION as u32);
        let units = date.1 % 10u64.pow(PRECISION as u32);
        let units_ns = units * 10u64.pow(9 - PRECISION as u32);
        Ok(date
            .0
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap())
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        let seconds = date.1 / 10u64.pow(date.2 as u32);
        let units = date.1 % 10u64.pow(date.2 as u32);
        let units_ns = units * 10u64.pow(9 - date.2 as u32);
        Ok(date
            .0
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap())
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

impl DynDateTime64 {
    pub fn try_from_tz(
        other: chrono::DateTime<Tz>,
        precision: usize,
    ) -> Result<Self, TryFromIntError> {
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = other.timestamp_subsec_nanos() as u64;
        let total =
            seconds * 10u64.pow(precision as u32) + sub_seconds / 10u64.pow(9 - precision as u32);
        Ok(Self(other.timezone(), total, precision))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        let seconds = date.1 / 10u64.pow(PRECISION as u32);
        let units = date.1 % 10u64.pow(PRECISION as u32);
        let units_ns = units * 10u64.pow(9 - PRECISION as u32);
        Ok(date
            .0
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap()
            .fixed_offset())
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        let seconds = date.1 / 10u64.pow(date.2 as u32);
        let units = date.1 % 10u64.pow(date.2 as u32);
        let units_ns = units * 10u64.pow(9 - date.2 as u32);
        Ok(date
            .0
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap()
            .fixed_offset())
    }
}

#[cfg(test)]
mod chrono_tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::UTC;

    #[test]
    #[allow(deprecated)]
    fn test_date() {
        for i in 0..30000u16 {
            let date = Date(i);
            let chrono_date: chrono::Date<Utc> = date.into();
            let new_date = Date::from(chrono_date);
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_naivedate() {
        for i in 0..30000u16 {
            let date = Date(i);
            let chrono_date: NaiveDate = date.into();
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
            assert_eq!(date_value, Value::DateTime64(DynDateTime64(UTC, i, 6)));
            let chrono_date: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(6, UTC), date_value).unwrap();
            let new_date = DateTime64::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64_precision2() {
        for i in (0..300u64).map(|x| x * 1000000) {
            let chrono_time = Utc.timestamp_opt(i as i64, i as u32).unwrap();
            let date = chrono_time.to_sql(None).unwrap();
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();
            assert_eq!(chrono_time, out_time);
            let date = match date {
                Value::DateTime64(mut datetime) => {
                    datetime.2 -= 3;
                    datetime.1 /= 1000;
                    Value::DateTime64(datetime)
                }
                _ => unimplemented!(),
            };
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();

            assert_eq!(chrono_time, out_time);
        }
    }

    #[test]
    #[allow(deprecated)]
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
