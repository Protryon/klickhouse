use crate::lock::ClickhouseLockHandle;
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

/// Acquires the global migration lock, forcefully resetting it if it's been held for over a minute.
async fn acquire_lock(lock: &ClickhouseLock) -> Result<ClickhouseLockHandle<'_>> {
    let start = Instant::now();
    loop {
        if let Some(handle) = lock.try_lock().await? {
            return Ok(handle);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        if start.elapsed() > Duration::from_secs(60) {
            lock.reset().await?;
        }
    }
}

/// Runs each migration's statements in order, holding `lock` (if any) for the duration.
async fn execute_migrations<'a, T: Iterator<Item = &'a str> + Send>(
    client: &Client,
    lock: Option<ClickhouseLock>,
    queries: T,
    substitute: impl Fn(&str) -> String + Send + Sync,
) -> Result<usize> {
    let handle = match &lock {
        Some(lock) => Some(acquire_lock(lock).await?),
        None => None,
    };
    let mut n = 0;
    for query in queries {
        n += 1;
        for query in query_parser::split_query_statements(query) {
            if query.is_empty() {
                continue;
            }
            Client::execute(client, substitute(&query)).await?;
        }
    }
    if let Some(handle) = handle {
        handle.unlock().await?;
    }
    Ok(n)
}

#[async_trait::async_trait]
impl AsyncTransaction for Client {
    type Error = KlickhouseError;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let lock = ClickhouseLock::new(self.clone(), "refinery_exec");
        execute_migrations(self, Some(lock), queries, str::to_string).await
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
///
/// Migrations may use the following placeholders:
/// * `%DATABASE%`: the configured database
/// * `%CLUSTER%`: the configured cluster name
/// * `%ON_CLUSTER%`: `ON CLUSTER <cluster name>`
///
/// Migrations that only use `%DATABASE%` and `%ON_CLUSTER%` can be shared with [`LocalMigration`].
pub struct ClusterMigration {
    client: Client,
    cluster_name: String,
    database: String,
    do_lock: bool,
}

impl ClusterMigration {
    pub fn new(client: Client, cluster_name: String, database: String) -> Self {
        Self {
            client,
            cluster_name,
            database,
            do_lock: true,
        }
    }

    pub fn without_lock(mut self) -> Self {
        self.do_lock = false;
        self
    }

    fn substitute(&self, query: &str) -> String {
        query
            .replace("%ON_CLUSTER%", &format!("ON CLUSTER {}", self.cluster_name))
            .replace("%DATABASE%", &self.database)
            .replace("%CLUSTER%", &self.cluster_name)
    }
}

#[async_trait::async_trait]
impl AsyncTransaction for ClusterMigration {
    type Error = KlickhouseError;
    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let lock = self.do_lock.then(|| {
            ClickhouseLock::new(self.client.clone(), "refinery_exec")
                .with_cluster(&self.cluster_name)
        });
        execute_migrations(&self.client, lock, queries, |query| self.substitute(query)).await
    }
}

#[async_trait::async_trait]
impl AsyncQuery<Vec<Migration>> for ClusterMigration {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let query = self.substitute(query);
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

/// Wrapper for Client to use migrations on a single, non-clustered node
///
/// Accepts the same migrations as [`ClusterMigration`], with the following placeholders:
/// * `%DATABASE%`: the configured database
/// * `%ON_CLUSTER%`: removed
///
/// `%CLUSTER%` is left as-is, so migrations that reference a cluster directly (i.e. `Distributed` or
/// `Replicated*` engines) cannot be shared and will fail.
pub struct LocalMigration {
    client: Client,
    database: String,
    do_lock: bool,
}

impl LocalMigration {
    pub fn new(client: Client, database: String) -> Self {
        Self {
            client,
            database,
            do_lock: true,
        }
    }

    pub fn without_lock(mut self) -> Self {
        self.do_lock = false;
        self
    }

    fn substitute(&self, query: &str) -> String {
        query
            .replace("%ON_CLUSTER%", "")
            .replace("%DATABASE%", &self.database)
    }
}

#[async_trait::async_trait]
impl AsyncTransaction for LocalMigration {
    type Error = KlickhouseError;
    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let lock = self
            .do_lock
            .then(|| ClickhouseLock::new(self.client.clone(), "refinery_exec"));
        execute_migrations(&self.client, lock, queries, |query| self.substitute(query)).await
    }
}

#[async_trait::async_trait]
impl AsyncQuery<Vec<Migration>> for LocalMigration {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let query = self.substitute(query);
        <Client as AsyncQuery<Vec<Migration>>>::query(&mut self.client, &query).await
    }
}

impl AsyncMigrate for LocalMigration {
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
