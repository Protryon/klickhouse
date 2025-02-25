use std::fmt;

use crate::{KlickhouseError, Result, ToSql, Value};

mod select;
pub use select::*;

#[derive(Debug, Clone)]
pub struct ParsedQuery(pub(crate) String);

impl fmt::Display for ParsedQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryInto<ParsedQuery> for String {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery(self))
    }
}

impl TryInto<ParsedQuery> for &str {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery(self.to_string()))
    }
}

impl TryInto<ParsedQuery> for &String {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery(self.clone()))
    }
}

#[derive(Clone)]
pub struct QueryBuilder<'a> {
    base: &'a str,
    arguments: Vec<Result<Value>>,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(query: &'a str) -> Self {
        Self {
            base: query,
            arguments: vec![],
        }
    }

    pub fn arg(mut self, arg: impl ToSql) -> Self {
        self.arguments.push(arg.to_sql(None));
        self
    }

    pub fn args<A: ToSql>(mut self, args: impl IntoIterator<Item = A>) -> Self {
        self.arguments
            .extend(args.into_iter().map(|x| x.to_sql(None)));
        self
    }

    pub fn finalize(self) -> Result<ParsedQuery> {
        self.try_into()
    }
}

impl TryInto<ParsedQuery> for QueryBuilder<'_> {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        let arguments = self.arguments.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(ParsedQuery(crate::query_parser::parse_query_arguments(
            self.base,
            &arguments[..],
        )))
    }
}
