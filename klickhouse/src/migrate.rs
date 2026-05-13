use crate::{ClickhouseLock, FromSql, query_parser};
use refinery_core::Migration;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use std::borrow::Cow;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::{Client, KlickhouseError, Result, Row, Type, Value};

impl Row for Migration {
    const COLUMN_COUNT: Option<usize> = Some(4);

    fn column_names() -> Option<Vec<Cow<'static, str>>> {
        Some(vec![
            "version".into(),
            "name".into(),
            "applied_on".into(),
            "checksum".into(),
        ])
    }

    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        if map.len() != 4 {
            return Err(KlickhouseError::DeserializeError(
                "bad column count for migration".to_string(),
            ));
        }
        let mut version = None::<i32>;
        let mut name_out = None::<String>;
        let mut applied_on = None::<String>;
        let mut checksum = None::<String>;
        for (name, type_, value) in map {
            match name {
                "version" => {
                    if version.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate version column".to_string(),
                        ));
                    }
                    version = Some(i32::from_sql(type_, value)?);
                }
                "name" => {
                    if name_out.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate name column".to_string(),
                        ));
                    }
                    name_out = Some(String::from_sql(type_, value)?);
                }
                "applied_on" => {
                    if applied_on.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate applied_on column".to_string(),
                        ));
                    }
                    applied_on = Some(String::from_sql(type_, value)?);
                }
                "checksum" => {
                    if checksum.is_some() {
                        return Err(KlickhouseError::DeserializeError(
                            "duplicate checksum column".to_string(),
                        ));
                    }
                    checksum = Some(String::from_sql(type_, value)?);
                }
                name => {
                    return Err(KlickhouseError::DeserializeError(format!(
                        "unexpected column {name}"
                    )));
                }
            }
        }
        if version.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing version".to_string(),
            ));
        }
        if name_out.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing name".to_string(),
            ));
        }
        if applied_on.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing applied_on".to_string(),
            ));
        }
        if checksum.is_none() {
            return Err(KlickhouseError::DeserializeError(
                "missing checksum".to_string(),
            ));
        }
        let applied_on =
            OffsetDateTime::parse(applied_on.as_ref().unwrap(), &Rfc3339).map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to parse time: {e:?}"))
            })?;

        Ok(Migration::applied(
            version.unwrap(),
            name_out.unwrap(),
            applied_on,
            checksum.unwrap().parse::<u64>().map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to parse checksum: {e:?}"))
            })?,
        ))
    }

    fn serialize_row(
        self,
        _type_hints: &indexmap::IndexMap<String, Type>,
    ) -> Result<Vec<(Cow<'static, str>, Value)>> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl AsyncTransaction for Client {
    type Error = KlickhouseError;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let lock = ClickhouseLock::new(self.clone(), "refinery_exec");
        let start = Instant::now();
        let handle = loop {
            if let Some(handle) = lock.try_lock().await? {
                break handle;
            } else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if start.elapsed() > Duration::from_secs(60) {
                    lock.reset().await?;
                }
            }
        };
        let mut n = 0;
        for query in queries {
            n += 1;
            if query.is_empty() {
                continue;
            }
            for query in query_parser::split_query_statements(query) {
                if query.is_empty() {
                    continue;
                }
                Client::execute(self, query).await?;
            }
        }
        handle.unlock().await?;
        Ok(n)
    }
}

#[async_trait::async_trait]
impl AsyncQuery<Vec<Migration>> for Client {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        self.query_collect::<Migration>(query).await
    }
}

impl AsyncMigrate for Client {
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {migration_table_name}(
            version INT,
            name VARCHAR(255),
            applied_on VARCHAR(255),
            checksum VARCHAR(255)) Engine=MergeTree() ORDER BY version;"
        )
    }
}

/// Wrapper for Client to use migrations on clusters
pub struct ClusterMigration {
    client: Client,
    cluster_name: String,
    database: String,
}

impl ClusterMigration {
    pub fn new(client: Client, cluster_name: String, database: String) -> Self {
        Self {
            client,
            cluster_name,
            database,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransaction for ClusterMigration {
    type Error = KlickhouseError;
    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let lock = ClickhouseLock::new(self.client.clone(), "refinery_exec")
            .with_cluster(&self.cluster_name);
        let start = Instant::now();
        let handle = loop {
            if let Some(handle) = lock.try_lock().await? {
                break handle;
            } else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if start.elapsed() > Duration::from_secs(60) {
                    lock.reset().await?;
                }
            }
        };
        let mut n = 0;
        for query in queries {
            n += 1;
            for query in query_parser::split_query_statements(query) {
                if query.is_empty() {
                    continue;
                }
                let query = query
                    .replace("%DATABASE%", &self.database)
                    .replace("%CLUSTER%", &self.cluster_name);
                Client::execute(&self.client, query).await?;
            }
        }
        handle.unlock().await?;
        Ok(n)
    }
}

#[async_trait::async_trait]
impl AsyncQuery<Vec<Migration>> for ClusterMigration {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let query = query
            .replace("%DATABASE%", &self.database)
            .replace("%CLUSTER%", &self.cluster_name);
        <Client as AsyncQuery<Vec<Migration>>>::query(&mut self.client, &query).await
    }
}

impl AsyncMigrate for ClusterMigration {
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        format!(
            r"CREATE TABLE IF NOT EXISTS {migration_table_name}_local ON CLUSTER %CLUSTER%(
                version INT,
                name VARCHAR(255),
                applied_on VARCHAR(255),
                checksum VARCHAR(255)
            ) Engine=MergeTree() ORDER BY version;
            CREATE TABLE IF NOT EXISTS
                {migration_table_name}
            ON CLUSTER %CLUSTER%
            AS {migration_table_name}_local ENGINE = Distributed(%CLUSTER%, %DATABASE%, {migration_table_name}_local, rand());
            ",
        )
    }
}
