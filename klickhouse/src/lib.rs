
pub const VERSION_MAJOR: u64 = 21;
pub const VERSION_MINOR: u64 = 6;

mod io;
mod internal_client_out;
mod internal_client_in;
mod protocol;
mod client;
mod block;
mod progress;
mod types;
mod values;
mod convert;
pub mod errors;

#[cfg(feature = "uuid")]
pub use uuid::Uuid;
#[cfg(not(feature = "uuid"))]
pub mod uuid;
#[cfg(not(feature = "uuid"))]
pub use uuid::Uuid;

pub use client::*;
pub use values::*;
pub use convert::{FromSql, ToSql, Row};
pub use types::Type;

pub use anyhow::{ Result, Error };