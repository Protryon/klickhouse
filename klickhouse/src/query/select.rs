use crate::{KlickhouseError, ParsedQuery, Result};

#[derive(Clone)]
pub struct SelectBuilder {
    withs: Vec<Result<ParsedQuery>>,
    distinct: bool,
    distinct_on: Vec<Result<ParsedQuery>>,
    exprs: Vec<Result<ParsedQuery>>,
    from: Result<ParsedQuery>,
    sample: Option<Result<ParsedQuery>>,
    array_joins: Vec<Result<ParsedQuery>>,
    joins: Vec<Result<ParsedQuery>>,
    prewhere: Vec<Result<ParsedQuery>>,
    where_: Vec<Result<ParsedQuery>>,
    group_by: Vec<Result<ParsedQuery>>,
    having: Vec<Result<ParsedQuery>>,
    order_by: Option<Result<ParsedQuery>>,
    limit: Option<Result<ParsedQuery>>,
    settings: Option<Result<ParsedQuery>>,
    union: Option<Result<ParsedQuery>>,
}

impl SelectBuilder {
    /// Creates a new [`SelectBuilder`] from the given FROM clause
    pub fn new(from: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        Self {
            from: from.try_into(),
            withs: Default::default(),
            distinct: Default::default(),
            distinct_on: Default::default(),
            exprs: Default::default(),
            sample: Default::default(),
            array_joins: Default::default(),
            joins: Default::default(),
            prewhere: Default::default(),
            where_: Default::default(),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            settings: Default::default(),
            union: Default::default(),
        }
    }

    /// Adds a new CTE to the query
    pub fn with(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.withs.push(item.try_into());
        self
    }

    /// Sets the distinct flag true/false. This clears `distinct_on` calls and vice versa, so don't mix them.
    pub fn distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self.distinct_on.clear();
        self
    }

    /// Adds some column names to a DISTINCT ON clause. This clears `distinct` calls and vice versa, so don't mix them.
    /// Names can be comma separated manually, or will be concatenated with commas.
    pub fn distinct_on(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.distinct = false;
        self.distinct_on.push(item.try_into());
        self
    }

    /// Adds an expression to the select clause. No trailing commas.
    pub fn select(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.exprs.push(item.try_into());
        self
    }

    /// Adds many expressions to the select clause. No trailing commas.
    pub fn select_all<I: TryInto<ParsedQuery, Error = KlickhouseError>>(
        mut self,
        items: impl IntoIterator<Item = I>,
    ) -> Self {
        for item in items {
            self.exprs.push(item.try_into());
        }
        self
    }

    /// Sets the SAMPLE clause. Overwrites previous SAMPLE clauses.
    pub fn sample(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.sample = Some(item.try_into());
        self
    }

    /// Adds an ARRAY JOIN clause. These must always be before JOIN clauses, so get their own section.
    /// Does not prefix "ARRAY JOIN" unlike other methods.
    pub fn array_join(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.array_joins.push(item.try_into());
        self
    }

    /// Adds a JOIN clause.
    /// Does not prefix "JOIN" due to optional prefixes.
    pub fn join(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.joins.push(item.try_into());
        self
    }

    /// Adds a PREWHERE clause. Concatenated automatically with AND operators.
    pub fn prewhere(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.prewhere.push(item.try_into());
        self
    }

    /// Adds multiple PREWHERE clause. Concatenated automatically with AND operators.
    pub fn prewhere_all<I: TryInto<ParsedQuery, Error = KlickhouseError>>(
        mut self,
        items: impl IntoIterator<Item = I>,
    ) -> Self {
        for item in items {
            self.prewhere.push(item.try_into());
        }
        self
    }

    /// Adds a WHERE clause. Concatenated automatically with AND operators.
    pub fn where_(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.where_.push(item.try_into());
        self
    }

    /// Adds multiple WHERE clauses. Concatenated automatically with AND operators.
    pub fn where_all<I: TryInto<ParsedQuery, Error = KlickhouseError>>(
        mut self,
        items: impl IntoIterator<Item = I>,
    ) -> Self {
        for item in items {
            self.where_.push(item.try_into());
        }
        self
    }

    /// Adds a column to the GROUP BY clause. No trailing commas. Can specify multiple in one call comma separated.
    pub fn group_by(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.group_by.push(item.try_into());
        self
    }

    /// Adds multiple columns to the GROUP BY clause. No trailing commas.
    pub fn group_by_all<I: TryInto<ParsedQuery, Error = KlickhouseError>>(
        mut self,
        items: impl IntoIterator<Item = I>,
    ) -> Self {
        for item in items {
            self.group_by.push(item.try_into());
        }
        self
    }

    /// Adds a HAVING clause. Concatenated automatically with AND operators.
    pub fn having(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.having.push(item.try_into());
        self
    }

    /// Adds multiple HAVING clauses. Concatenated automatically with AND operators.
    pub fn having_all<I: TryInto<ParsedQuery, Error = KlickhouseError>>(
        mut self,
        items: impl IntoIterator<Item = I>,
    ) -> Self {
        for item in items {
            self.having.push(item.try_into());
        }
        self
    }

    /// Sets the ORDER BY clause. Overwrites previous ORDER BY clauses.
    pub fn order_by(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.order_by = Some(item.try_into());
        self
    }

    /// Sets the LIMIT clause. Overwrites previous LIMIT clauses.
    pub fn limit(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.limit = Some(item.try_into());
        self
    }

    /// Sets the SETTINGS clause. Overwrites previous SETTINGS clauses.
    pub fn settings(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.settings = Some(item.try_into());
        self
    }

    /// Sets the UNION clause. Overwrites previous UNION clauses.
    pub fn union(mut self, item: impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Self {
        self.union = Some(item.try_into());
        self
    }

    /// Builds this SelectBuilder into a ParsedQuery
    pub fn build(self) -> Result<ParsedQuery> {
        self.try_into()
    }
}

impl TryInto<ParsedQuery> for SelectBuilder {
    type Error = KlickhouseError;

    fn try_into(mut self) -> Result<ParsedQuery> {
        let mut out = String::new();

        if !self.withs.is_empty() {
            out.push_str("WITH ");
            self.withs.reverse();
            while let Some(last) = self.withs.pop() {
                let last = last?;
                out.push_str(&last.0);
                if !self.withs.is_empty() {
                    out.push(',');
                }
            }
            out.push('\n');
        }

        out.push_str("SELECT\n");

        if self.distinct {
            out.push_str("DISTINCT\n");
        } else if !self.distinct_on.is_empty() {
            out.push_str("DISTINCT ON (");
            self.distinct_on.reverse();
            while let Some(last) = self.distinct_on.pop() {
                let last = last?;
                out.push_str(&last.0);
                if !self.distinct_on.is_empty() {
                    out.push(',');
                }
            }
            out.push_str(")\n");
        }

        self.exprs.reverse();
        while let Some(last) = self.exprs.pop() {
            let last = last?;
            out.push_str(&last.0);
            if !self.exprs.is_empty() {
                out.push_str(",\n");
            } else {
                out.push('\n');
            }
        }

        out.push_str("FROM ");
        out.push_str(&self.from?.0);
        out.push('\n');
        if let Some(sample) = self.sample {
            out.push_str("SAMPLE ");
            out.push_str(&sample?.0);
            out.push('\n');
        }

        if !self.array_joins.is_empty() {
            self.array_joins.reverse();
            while let Some(last) = self.array_joins.pop() {
                let last = last?;
                out.push_str(&last.0);
                out.push('\n');
            }
        }

        if !self.joins.is_empty() {
            self.joins.reverse();
            while let Some(last) = self.joins.pop() {
                let last = last?;
                out.push_str(&last.0);
                out.push('\n');
            }
        }

        if !self.prewhere.is_empty() {
            self.prewhere.reverse();
            out.push_str("PREWHERE (");
            while let Some(last) = self.prewhere.pop() {
                let last = last?;
                out.push_str(&last.0);
                if !self.prewhere.is_empty() {
                    out.push_str(") AND\n(");
                } else {
                    out.push_str(")\n");
                }
            }
        }

        if !self.where_.is_empty() {
            self.where_.reverse();
            out.push_str("WHERE (");
            while let Some(last) = self.where_.pop() {
                let last = last?;
                out.push_str(&last.0);
                if !self.where_.is_empty() {
                    out.push_str(") AND\n(");
                } else {
                    out.push_str(")\n");
                }
            }
        }

        if !self.group_by.is_empty() {
            self.group_by.reverse();
            out.push_str("GROUP BY ");
            while let Some(last) = self.group_by.pop() {
                let last = last?;
                out.push_str(&last.0);
                if !self.group_by.is_empty() {
                    out.push_str(",\n");
                } else {
                    out.push('\n');
                }
            }
        }

        if !self.having.is_empty() {
            self.having.reverse();
            out.push_str("HAVING (");
            while let Some(last) = self.having.pop() {
                let last = last?;
                out.push_str(&last.0);
                if !self.having.is_empty() {
                    out.push_str(") AND\n(");
                } else {
                    out.push_str(")\n");
                }
            }
        }

        if let Some(order_by) = self.order_by {
            out.push_str("ORDER BY ");
            out.push_str(&order_by?.0);
            out.push('\n');
        }

        if let Some(limit) = self.limit {
            out.push_str("LIMIT ");
            out.push_str(&limit?.0);
            out.push('\n');
        }

        if let Some(settings) = self.settings {
            out.push_str("SETTINGS ");
            out.push_str(&settings?.0);
            out.push('\n');
        }

        if let Some(union) = self.union {
            out.push_str("UNION ");
            out.push_str(&union?.0);
            out.push('\n');
        }

        Ok(ParsedQuery(out))
    }
}

#[cfg(test)]
mod tests {
    use crate::QueryBuilder;

    use super::*;

    #[test]
    fn test_select_builder() {
        let builder = SelectBuilder::new("table_name")
            .select("col1")
            .select("col2 as COL2")
            .array_join("ARRAY JOIN col3")
            .where_("col4 LIKE 'test'")
            .group_by("col1")
            .where_(QueryBuilder::new("col5 = $1").arg("test"));

        let query = builder.build().unwrap();
        println!("{query}");
    }
}
