use std::string::FromUtf8Error;

use thiserror::Error;

use crate::Type;

#[derive(Error, Debug)]
pub enum KlickhouseError {
    #[error("no rows received when expecting at least one row")]
    MissingRow,
    #[error("can't fetch the same column twice from RawRow")]
    DoubleFetch,
    #[error("column index was out of bounds or not present")]
    OutOfBounds,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("duplicate field {0} in struct")]
    DuplicateField(&'static str),
    #[error("protocol error: {0}")]
    ProtocolError(String),
    #[error("type parse error: {0}")]
    TypeParseError(String),
    #[error("deserialize error: {0}")]
    DeserializeError(String),
    #[error("server exception: {code} {name}: {message}\n{stack_trace}")]
    ServerException {
        code: i32,
        name: String,
        message: String,
        stack_trace: String,
    },
    #[error("unexpected type: {0}")]
    UnexpectedType(Type),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("utf-8 conversion error: {0}")]
    Utf8(#[from] FromUtf8Error),
}

pub type Result<T, E = KlickhouseError> = std::result::Result<T, E>;
