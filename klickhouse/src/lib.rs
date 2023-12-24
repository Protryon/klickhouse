#![doc = include_str!("../../README.md")]

/// Clickhouse major version
pub const VERSION_MAJOR: u64 = 22;
/// Clickhouse minor version
pub const VERSION_MINOR: u64 = 9;

pub mod block;
mod client;
#[cfg(feature = "compression")]
mod compression;
mod convert;
/// Error generator functions used by `klickhouse_derive`
mod errors;
mod internal_client_in;
mod internal_client_out;
mod io;
#[cfg(feature = "bb8")]
mod manager;
#[cfg(feature = "refinery")]
mod migrate;
#[cfg(feature = "refinery")]
pub use migrate::*;
mod progress;
pub use progress::*;
mod protocol;
mod query;
pub mod query_parser;
mod types;
mod values;
pub use query::*;

#[cfg(feature = "bb8")]
pub use manager::ConnectionManager;

pub use uuid::Uuid;

#[cfg(feature = "derive")]
pub use klickhouse_derive::Row;

pub use client::*;
pub use convert::*;
pub use errors::*;
pub use types::{Type, Tz};
pub use values::*;
mod lock;
pub use lock::ClickhouseLock;
