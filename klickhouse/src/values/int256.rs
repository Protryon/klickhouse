use crate::{Value, convert::{FromSql, ToSql, unexpected_type}, types::Type};
use anyhow::*;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct i256(pub [u8; 32]);

impl Into<u256> for i256 {
    fn into(self) -> u256 {
        u256(self.0)
    }
}

impl ToSql for i256 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::Int256(self))
    }
}

impl FromSql for i256 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Int256) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Int256(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl Into<(u128, u128)> for i256 {
    fn into(self) -> (u128, u128) {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&self.0[..16]);
        let n1 = u128::from_be_bytes(buf);
        buf.copy_from_slice(&self.0[16..]);
        let n2 = u128::from_be_bytes(buf);
        (n1, n2)
    }
}

impl From<(u128, u128)> for i256 {
    fn from(other: (u128, u128)) -> Self {
        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&other.0.to_be_bytes()[..]);
        buf[16..].copy_from_slice(&other.1.to_be_bytes()[..]);
        i256(buf)
    }
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct u256(pub [u8; 32]);


impl ToSql for u256 {
    fn to_sql(self) -> Result<Value> {
        Ok(Value::UInt256(self))
    }
}

impl FromSql for u256 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::UInt256) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::UInt256(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl Into<i256> for u256 {
    fn into(self) -> i256 {
        i256(self.0)
    }
}

impl Into<(u128, u128)> for u256 {
    fn into(self) -> (u128, u128) {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&self.0[..16]);
        let n1 = u128::from_be_bytes(buf);
        buf.copy_from_slice(&self.0[16..]);
        let n2 = u128::from_be_bytes(buf);
        (n1, n2)
    }
}

impl From<(u128, u128)> for u256 {
    fn from(other: (u128, u128)) -> Self {
        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&other.0.to_be_bytes()[..]);
        buf[16..].copy_from_slice(&other.1.to_be_bytes()[..]);
        u256(buf)
    }
}