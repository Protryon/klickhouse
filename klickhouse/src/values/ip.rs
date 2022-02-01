use super::*;
use std::{
    fmt,
    net::{Ipv4Addr, Ipv6Addr},
    ops::Deref,
};

/// Wrapper type for Clickhouse `IPv4` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ipv4(pub Ipv4Addr);

impl fmt::Display for Ipv4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for Ipv4 {
    type Target = Ipv4Addr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Ipv4> for Ipv4Addr {
    fn from(i: Ipv4) -> Self {
        i.0
    }
}

impl From<Ipv4Addr> for Ipv4 {
    fn from(x: Ipv4Addr) -> Self {
        Self(x)
    }
}

impl Default for Ipv4 {
    fn default() -> Self {
        Self(Ipv4Addr::UNSPECIFIED)
    }
}

/// Wrapper type for Clickhouse `IPv6` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ipv6(pub Ipv6Addr);

impl fmt::Display for Ipv6 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for Ipv6 {
    type Target = Ipv6Addr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Ipv6> for Ipv6Addr {
    fn from(i: Ipv6) -> Self {
        i.0
    }
}

impl From<Ipv6Addr> for Ipv6 {
    fn from(x: Ipv6Addr) -> Self {
        Self(x)
    }
}

impl Default for Ipv6 {
    fn default() -> Self {
        Self(Ipv6Addr::UNSPECIFIED)
    }
}

impl ToSql for Ipv4 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Ipv4(self))
    }
}

impl FromSql for Ipv4 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Ipv4) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Ipv4(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl ToSql for Ipv6 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Ipv6(self))
    }
}

impl FromSql for Ipv6 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Ipv6) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Ipv6(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}
