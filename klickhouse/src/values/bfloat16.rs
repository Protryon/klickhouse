//! BFloat16 support for ClickHouse values
//!
//! This module provides BFloat16 functionality when the `bfloat16` feature is enabled.
//! When disabled, it provides stub implementations to maintain API compatibility.

#[cfg(feature = "bfloat16")]
pub use half::bf16;

use crate::Value;

/// Stub BFloat16 type used when the `bfloat16` feature is disabled
#[cfg(not(feature = "bfloat16"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(non_camel_case_types)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct bf16;

#[cfg(not(feature = "bfloat16"))]
impl bf16 {
    /// Stub implementation that returns zero
    pub fn to_bits(self) -> u16 {
        0
    }

    /// Stub implementation that ignores input
    pub fn from_bits(_bits: u16) -> Self {
        bf16
    }

    /// Stub implementation that returns zero
    pub fn to_f32(self) -> f32 {
        0.0
    }

    /// Stub implementation that ignores input
    pub fn from_f32(_value: f32) -> Self {
        bf16
    }

    /// Stub constant
    pub const ZERO: bf16 = bf16;
    pub const ONE: bf16 = bf16;
    pub const NAN: bf16 = bf16;
    pub const INFINITY: bf16 = bf16;
    pub const NEG_INFINITY: bf16 = bf16;

    /// Stub implementation
    pub fn is_nan(self) -> bool {
        false
    }
}

#[cfg(not(feature = "bfloat16"))]
impl std::fmt::Display for bf16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<bfloat16 disabled>")
    }
}

/// Creates a default BFloat16 value
#[cfg(feature = "bfloat16")]
pub fn default_bf16_value() -> Value {
    Value::BFloat16(bf16::ZERO)
}

/// Error stub for default value when feature is disabled
#[cfg(not(feature = "bfloat16"))]
pub fn default_bf16_value() -> Value {
    panic!("BFloat16 support is disabled. Enable the 'bfloat16' feature to use BFloat16 types.")
}

/// Deserializes BFloat16 from bits when feature is enabled
#[cfg(feature = "bfloat16")]
pub fn deserialize_bf16_from_bits(bits: u16) -> Value {
    Value::BFloat16(bf16::from_bits(bits))
}

/// Error stub for deserialization when feature is disabled
#[cfg(not(feature = "bfloat16"))]
pub fn deserialize_bf16_from_bits(_bits: u16) -> Value {
    panic!("BFloat16 support is disabled. Enable the 'bfloat16' feature to deserialize BFloat16 values.")
}

/// Serializes BFloat16 to bits when feature is enabled
#[cfg(feature = "bfloat16")]
pub fn serialize_bf16_to_bits(value: &bf16) -> u16 {
    value.to_bits()
}

/// Error stub for serialization when feature is disabled
#[cfg(not(feature = "bfloat16"))]
pub fn serialize_bf16_to_bits(_value: &bf16) -> u16 {
    panic!(
        "BFloat16 support is disabled. Enable the 'bfloat16' feature to serialize BFloat16 values."
    )
}

/// Hashes a BFloat16 value
#[cfg(feature = "bfloat16")]
pub fn hash_bf16<H: std::hash::Hasher>(value: &bf16, state: &mut H) {
    std::hash::Hash::hash(&value.to_bits(), state);
}

/// Stub hash when feature is disabled
#[cfg(not(feature = "bfloat16"))]
pub fn hash_bf16<H: std::hash::Hasher>(_value: &bf16, state: &mut H) {
    std::hash::Hash::hash(&0u16, state);
}

/// Checks if the bfloat16 feature is enabled at compile time
pub const fn is_bfloat16_enabled() -> bool {
    cfg!(feature = "bfloat16")
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "bfloat16")]
    use crate::{FromSql, ToSql, Type};
    
    use super::*;

    #[test]
    fn test_feature_detection() {
        assert_eq!(is_bfloat16_enabled(), cfg!(feature = "bfloat16"));
    }

    #[cfg(feature = "bfloat16")]
    #[test]
    fn test_bf16_functionality() {
        let value = bf16::from_f32(42.5);
        assert_eq!(value.to_f32(), 42.5);

        let bits = value.to_bits();
        let restored = bf16::from_bits(bits);
        assert_eq!(restored.to_bits(), value.to_bits());
    }

    #[cfg(feature = "bfloat16")]
    #[test]
    fn test_bf16_to_from_sql() {
        let value = bf16::from_f32(123.456);

        // Test ToSql
        let sql_value = value.to_sql(None).unwrap();
        match sql_value {
            Value::BFloat16(v) => assert_eq!(v.to_bits(), value.to_bits()),
            _ => panic!("Expected BFloat16 value"),
        }

        // Test FromSql
        let restored = bf16::from_sql(&Type::BFloat16, Value::BFloat16(value)).unwrap();
        assert_eq!(restored.to_bits(), value.to_bits());

        // Test special values
        let special_values = [
            bf16::from_f32(0.0),
            bf16::from_f32(-0.0),
            bf16::from_f32(1.0),
            bf16::from_f32(-1.0),
            bf16::INFINITY,
            bf16::NEG_INFINITY,
            bf16::NAN,
        ];

        for &val in &special_values {
            let sql_val = val.to_sql(None).unwrap();
            let restored_val = bf16::from_sql(&Type::BFloat16, sql_val).unwrap();
            if val.is_nan() {
                assert!(restored_val.is_nan());
            } else {
                assert_eq!(restored_val.to_bits(), val.to_bits());
            }
        }
    }

    #[cfg(not(feature = "bfloat16"))]
    #[test]
    fn test_bf16_stub() {
        let value = bf16;
        assert_eq!(value.to_bits(), 0);
        assert_eq!(value.to_f32(), 0.0);
        assert!(!value.is_nan());
    }
}
