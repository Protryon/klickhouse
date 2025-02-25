use std::fmt::{self, Display};
use syn::{Ident, Path};

#[derive(Copy, Clone)]
pub struct Symbol(&'static str);

pub const BOUND: Symbol = Symbol("bound");
pub const DEFAULT: Symbol = Symbol("default");
pub const DENY_UNKNOWN_FIELDS: Symbol = Symbol("deny_unknown_fields");
pub const NESTED: Symbol = Symbol("nested");
pub const FLATTEN: Symbol = Symbol("flatten");
pub const DESERIALIZE_WITH: Symbol = Symbol("deserialize_with");
pub const FROM: Symbol = Symbol("from");
pub const INTO: Symbol = Symbol("into");
pub const RENAME: Symbol = Symbol("rename");
pub const RENAME_ALL: Symbol = Symbol("rename_all");
pub const KLICKHOUSE: Symbol = Symbol("klickhouse");
pub const SERIALIZE_WITH: Symbol = Symbol("serialize_with");
pub const SKIP: Symbol = Symbol("skip");
pub const SKIP_DESERIALIZING: Symbol = Symbol("skip_deserializing");
pub const SKIP_SERIALIZING: Symbol = Symbol("skip_serializing");
pub const TRY_FROM: Symbol = Symbol("try_from");
pub const WITH: Symbol = Symbol("with");

impl PartialEq<Symbol> for Ident {
    fn eq(&self, word: &Symbol) -> bool {
        self == word.0
    }
}

impl PartialEq<Symbol> for &Ident {
    fn eq(&self, word: &Symbol) -> bool {
        *self == word.0
    }
}

impl PartialEq<Symbol> for Path {
    fn eq(&self, word: &Symbol) -> bool {
        self.is_ident(word.0)
    }
}

impl PartialEq<Symbol> for &Path {
    fn eq(&self, word: &Symbol) -> bool {
        self.is_ident(word.0)
    }
}

impl Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(self.0)
    }
}
