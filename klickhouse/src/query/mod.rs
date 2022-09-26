use crate::{KlickhouseError, Result, ToSql, Value};

pub struct ParsedQuery(pub(crate) String);

impl TryInto<ParsedQuery> for String {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery(self))
    }
}

impl<'a> TryInto<ParsedQuery> for &'a str {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery(self.to_string()))
    }
}

impl<'a> TryInto<ParsedQuery> for &'a String {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery(self.clone()))
    }
}

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
        self.arguments.push(arg.to_sql());
        self
    }

    pub fn args<A: ToSql>(mut self, args: impl IntoIterator<Item = A>) -> Self {
        self.arguments.extend(args.into_iter().map(ToSql::to_sql));
        self
    }
}

impl<'a> TryInto<ParsedQuery> for QueryBuilder<'a> {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        let arguments = self.arguments.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(ParsedQuery(crate::query_parser::parse_query_arguments(
            self.base,
            &arguments[..],
        )))
    }
}
