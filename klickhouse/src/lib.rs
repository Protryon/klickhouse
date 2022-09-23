/// Clickhouse major version
pub const VERSION_MAJOR: u64 = 21;
/// Clickhouse minor version
pub const VERSION_MINOR: u64 = 6;

mod block;
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
mod progress;
mod protocol;
mod types;
mod values;

#[cfg(feature = "bb8")]
pub use manager::ConnectionManager;

pub use uuid::Uuid;

#[cfg(feature = "derive")]
pub use klickhouse_derive::Row;

pub use client::*;
pub use convert::{FromSql, Row, ToSql};
pub use errors::*;
pub use types::Type;
pub use values::*;
