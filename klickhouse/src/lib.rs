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
/// Derive macro for the [Row] trait.
///
/// This is similar in usage and implementation to the [serde::Serialize] and [serde::Deserialize] derive macros.
///
/// ## serde attributes
/// The following [serde attributes](https://serde.rs/attributes.html) are supported, using `#[klickhouse(...)]` instead of `#[serde(...)]`:
/// - `with`
/// - `from` and `into`
/// - `try_from`
/// - `skip`
/// - `default`
/// - `deny_unknown_fields`
/// - `rename`
/// - `rename_all`
/// - `serialize_with`, `deserialize_with`
/// - `skip_deserializing`, `skip_serializing`
/// - `flatten`
///    - Index-based matching is disabled (the column names must match exactly).
///    - Due to the current interface of the [Row] trait, performance might not be optimal, as a value map must be reconstitued for each flattened subfield.
///
/// ## Clickhouse-specific attributes
/// - The `nested` attribute allows handling [Clickhouse nested data structures](https://clickhouse.com/docs/en/sql-reference/data-types/nested-data-structures/nested). See an example in the `tests` folder.
///
/// ## Known issues
/// - For serialization, the ordering of fields in the struct declaration must match the order in the `INSERT` statement, respectively in the table declaration. See issue [#34](https://github.com/Protryon/klickhouse/issues/34).
pub use klickhouse_derive::Row;

pub use client::*;
pub use convert::*;
pub use errors::*;
pub use types::{Type, Tz};
pub use values::*;
mod lock;
pub use lock::ClickhouseLock;
