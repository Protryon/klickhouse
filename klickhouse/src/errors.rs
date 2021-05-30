use anyhow::*;

pub fn missing_field(name: &'static str) -> Error {
    anyhow!("missing field '{}' from struct", name)
}

pub fn duplicate_field(name: &'static str) -> Error {
    anyhow!("duplicate field '{}' in struct", name)
}
