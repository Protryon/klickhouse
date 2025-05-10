use crate::Value;
use compiler_tools::util::parse_str;
use compiler_tools::TokenParse;
use compiler_tools_derive::token_parse;
use std::fmt::Write;

fn parse_heredoc(input: &str) -> Option<(&str, &str)> {
    let (tag, remaining) = parse_str::<'$'>(input)?;

    let reoccur_index = remaining.find(tag)?;

    Some((
        &input[..tag.len() * 2 + reoccur_index],
        &input[tag.len() * 2 + reoccur_index..],
    ))
}

#[token_parse]
#[derive(PartialEq, Clone, Copy, Debug)]
enum Token<'a> {
    #[token(regex = "$[0-9]+")]
    ClientArgument(&'a str),
    #[token(parse_fn = "parse_heredoc")]
    Heredoc(&'a str),
    #[token(regex = "--[^\n]*")]
    CommentDash(&'a str),
    #[token(regex = "/\\*.*\\*/")]
    CommentBlock(&'a str),

    OpeningRoundBracket = "(",
    ClosingRoundBracket = ")",
    OpeningSquareBracket = "[",
    ClosingSquareBracket = "]",
    OpeningCurlyBrace = "{",
    ClosingCurlyBrace = "}",
    Comma = ",",
    Semicolon = ";",
    VerticalDelimiter = "\\g",
    Dot = ".",
    Asterisk = "*",
    Plus = "+",
    Minus = "-",
    Slash = "/",
    Percent = "%",
    Arrow = "->",
    QuestionMark = "?",
    Colon = ":",
    DoubleColon = "::",
    #[token(literal = "=")]
    Equals(&'a str) = "==",
    #[token(literal = "<>")]
    NotEquals(&'a str) = "!=",
    Less = "<",
    Greater = ">",
    LessOrEquals = "<=",
    GreaterOrEquals = ">=",
    Concatenation = "||",
    At = "@",
    DoubleAt = "@@",
    EscapedDollarSign = "$$",
    DollarSign = "$",

    #[token(regex = "[ \n\t\r\x0C\x0B]+")]
    Whitespace(&'a str),
    #[token(regex = "#![^\n]*")]
    CommentHashbang(&'a str),
    #[token(regex = "#[^\n]*")]
    CommentHash(&'a str),
    #[token(regex = "[a-zA-Z_][0-9a-zA-Z_]*")]
    BareWord(&'a str),
    #[token(
        regex_full = "(?i)0x[0-9a-f]+(\\.[0-9a-f]*)(p[+-]?[0-9]+)|0[0-7]+|[0-9]+(\\.[0-9]+|\\.)?(e[+-]?[0-9]+)?|\\.[0-9]+(e[+-]?[0-9]+)?|inf|infinity|nan"
    )]
    Number(&'a str),
    #[token(parse_fn = "compiler_tools::util::parse_str::<'\\''>")]
    StringLiteral(&'a str),
    #[token(parse_fn = "compiler_tools::util::parse_str::<'`'>")]
    QuotedIdentifierBacktick(&'a str),
    #[token(parse_fn = "compiler_tools::util::parse_str::<'\"'>")]
    QuotedIdentifierDoubleQuote(&'a str),
    #[token(illegal)]
    Illegal(char),
}

/// Parses a query and replaces arguments with values
pub fn parse_query_arguments(query: &str, arguments: &[Value]) -> String {
    let mut tokenizer = Tokenizer::new(query);
    let mut out = String::with_capacity(query.len() + 100);
    while let Some(token) = tokenizer.next() {
        match token.token {
            Token::EscapedDollarSign => write!(&mut out, "{}", Token::DollarSign).unwrap(),
            Token::ClientArgument(argument) => match argument[1..].parse::<usize>() {
                Ok(index) if index <= arguments.len() && index > 0 => {
                    write!(&mut out, "{}", arguments[index - 1]).unwrap()
                }
                _ => write!(&mut out, "{}", token.token).unwrap(),
            },
            t => write!(&mut out, "{t}").unwrap(),
        }
    }
    out
}

/// Splits a series of semicolon-delimited queries into individual queries
pub fn split_query_statements(query: &str) -> Vec<String> {
    let mut tokenizer = Tokenizer::new(query);
    let mut out = vec![String::new()];
    while let Some(token) = tokenizer.next() {
        match token.token {
            Token::Semicolon => {
                write!(out.last_mut().unwrap(), "{}", Token::Semicolon).unwrap();

                out.push(String::new());
            }
            token => {
                write!(out.last_mut().unwrap(), "{token}").unwrap();
            }
        }
    }
    out.into_iter()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use compiler_tools::TokenParse;

    #[test]
    fn parse_tests() {
        let tests = &[
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY *",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY *",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') APPLY(x)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') REPLACE(y)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * APPLY(x)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY * REPLACE(y)",
            "OPTIMIZE TABLE table_name DEDUPLICATE BY db.a, db.b, db.c",
            "MODIFY COMMENT ''",
            "MODIFY COMMENT ''",
            "MODIFY COMMENT 'some comment value'",
            "MODIFY COMMENT 'some comment value'",
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw')",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')",
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE `tbl`\n(PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE `tbl`\n(\n    PARTITION BY toYYYYMM(`created`)\n)",
            "CREATE DATABASE db ENGINE=Foo TABLE OVERRIDE `tbl` (), TABLE OVERRIDE a (COLUMNS (_created DateTime MATERIALIZED now())), TABLE OVERRIDE b (PARTITION BY rand())",
            "CREATE DATABASE db\nENGINE = Foo\nTABLE OVERRIDE `tbl`,\nTABLE OVERRIDE `a`\n(\n    COLUMNS\n    (\n        `_created` DateTime MATERIALIZED now()\n    )\n),\nTABLE OVERRIDE `b`\n(\n    PARTITION BY rand()\n)",
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE tbl (COLUMNS (id UUID) PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE `tbl`\n(\n    COLUMNS\n    (\n        `id` UUID\n    )\n    PARTITION BY toYYYYMM(`created`)\n)",
            "CREATE DATABASE db TABLE OVERRIDE tbl (COLUMNS (INDEX foo foo TYPE minmax GRANULARITY 1) PARTITION BY if(_staged = 1, 'staging', toYYYYMM(created)))",
            "CREATE DATABASE db\nTABLE OVERRIDE `tbl`\n(\n    COLUMNS\n    (\n        INDEX foo `foo` TYPE minmax GRANULARITY 1\n    )\n    PARTITION BY if(`_staged` = 1, 'staging', toYYYYMM(`created`))\n)",
            "CREATE DATABASE db TABLE OVERRIDE t1 (TTL inserted + INTERVAL 1 MONTH DELETE), TABLE OVERRIDE t2 (TTL `inserted` + INTERVAL 2 MONTH DELETE)",
            "CREATE DATABASE db\nTABLE OVERRIDE `t1`\n(\n    TTL `inserted` + toIntervalMonth(1)\n),\nTABLE OVERRIDE `t2`\n(\n    TTL `inserted` + toIntervalMonth(2)\n)",
            "CREATE DATABASE db ENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw') SETTINGS allows_query_when_mysql_lost = 1 TABLE OVERRIDE tab3 (COLUMNS (_staged UInt8 MATERIALIZED 1) PARTITION BY (c3) TTL c3 + INTERVAL 10 minute), TABLE OVERRIDE tab5 (PARTITION BY (c3) TTL c3 + INTERVAL 10 minute)",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw')\nSETTINGS allows_query_when_mysql_lost = 1\nTABLE OVERRIDE `tab3`\n(\n    COLUMNS\n    (\n        `_staged` UInt8 MATERIALIZED 1\n    )\n    PARTITION BY `c3`\n    TTL `c3` + toIntervalMinute(10)\n),\nTABLE OVERRIDE `tab5`\n(\n    PARTITION BY `c3`\n    TTL `c3` + toIntervalMinute(10)\n)",
            "CREATE DATABASE db TABLE OVERRIDE tbl (PARTITION BY toYYYYMM(created) COLUMNS (created DateTime CODEC(Delta)))",
            "CREATE DATABASE db\nTABLE OVERRIDE `tbl`\n(\n    COLUMNS\n    (\n        `created` DateTime CODEC(Delta)\n    )\n    PARTITION BY toYYYYMM(`created`)\n)",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2 TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2\nTABLE OVERRIDE `a`\n(\n    ORDER BY (`id`, `version`)\n)",
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2 COMMENT 'db comment' TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2\nTABLE OVERRIDE `a`\n(\n    ORDER BY (`id`, `version`)\n)\nCOMMENT 'db comment'",
            "CREATE USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'",
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '[A-Za-z0-9]{64}' SALT '[A-Za-z0-9]{64}'",
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "ALTER USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'",
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '[A-Za-z0-9]{64}' SALT '[A-Za-z0-9]{64}'",
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "CREATE USER user1 IDENTIFIED WITH sha256_password BY 'qwe123' SALT 'EFFD7F6B03B3EA68B8F86C1E91614DD50E42EB31EF7160524916444D58B5E264'",
            "ATTACH USER user1 IDENTIFIED WITH sha256_hash BY '2CC4880302693485717D34E06046594CFDFE425E3F04AA5A094C4AABAB3CB0BF' SALT 'EFFD7F6B03B3EA68B8F86C1E91614DD50E42EB31EF7160524916444D58B5E264';",
            "ATTACH USER user1 IDENTIFIED WITH sha256_hash BY '2CC4880302693485717D34E06046594CFDFE425E3F04AA5A094C4AABAB3CB0BF'",
            "$HTEST$SS$HTEST$",
            "$2$3$$$",
            "$$2$3",
            "1 - 2 --sdfsdfsdf",
        ];

        for test in tests {
            let mut tokenizer = Tokenizer::new(test);
            while let Some(token) = tokenizer.next() {
                println!("{token}");
                assert!(!matches!(token.token, Token::Illegal(_)));
            }
            println!();
        }
    }

    #[test]
    fn arg_tests() {
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $2",
                &[Value::string("te'st"), Value::UInt32(3232)]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = 3232"
        );
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $3",
                &[Value::string("te'st"), Value::UInt32(3232)]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = $3"
        );
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $1",
                &[Value::string("te'st"), Value::UInt32(3232)]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = 'te\\'st'"
        );
        assert_eq!(
            parse_query_arguments(
                "SELECT a, b FROM x WHERE x.y = $1 AND x.z = $0",
                &[Value::string("te'st")]
            ),
            "SELECT a, b FROM x WHERE x.y = 'te\\'st' AND x.z = $0"
        );
    }

    #[test]
    fn split_tests() {
        assert_eq!(split_query_statements("X;B",), vec!["X;", "B"]);
        assert_eq!(split_query_statements("X;B;",), vec!["X;", "B;"]);
        assert_eq!(split_query_statements("X;B;\n",), vec!["X;", "B;"]);
        assert_eq!(split_query_statements("X;B;\n\n\n",), vec!["X;", "B;"]);
        assert_eq!(split_query_statements("X;\n\n\n",), vec!["X;"]);
        assert_eq!(split_query_statements("X\n\n\n",), vec!["X"]);
        assert_eq!(split_query_statements("",), Vec::<&str>::new());
    }
}
