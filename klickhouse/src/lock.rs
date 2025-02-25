use std::time::Duration;

use log::error;

use crate::{Client, KlickhouseError};

/// A hack implementation of a global lock for things like migrations
#[derive(Clone)]
pub struct ClickhouseLock {
    name: String,
    cluster_str: String,
    client: Client,
}

/// A handle, that when dropped, attempts to unlock the owning lock.
pub struct ClickhouseLockHandle<'a> {
    lock: Option<&'a ClickhouseLock>,
}

impl ClickhouseLock {
    /// Initialize a new lock.
    pub fn new(client: Client, name: impl AsRef<str>) -> Self {
        Self {
            name: name.as_ref().to_string(),
            client,
            cluster_str: String::new(),
        }
    }

    pub fn with_cluster(mut self, cluster: impl AsRef<str>) -> Self {
        self.cluster_str = format!(" ON CLUSTER {}", cluster.as_ref());
        self
    }

    /// Attempts to lock this table a single time.
    pub async fn try_lock(&self) -> Result<Option<ClickhouseLockHandle<'_>>, KlickhouseError> {
        let query = format!(
            "CREATE TABLE _lock_{}{} (i Int64)ENGINE=Null",
            self.name, self.cluster_str
        );

        match self.client.execute(&query).await {
            Ok(()) => (),
            Err(e) => {
                let error = e.to_string();
                if error.contains("already exists") {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
        }

        Ok(Some(ClickhouseLockHandle { lock: Some(self) }))
    }

    /// Attempts to lock this table.
    pub async fn lock(&self) -> Result<ClickhouseLockHandle<'_>, KlickhouseError> {
        let query = format!(
            "CREATE TABLE _lock_{}{} (i Int64)ENGINE=Null",
            self.name, self.cluster_str
        );

        loop {
            match self.client.execute(&query).await {
                Ok(()) => break,
                Err(e) => {
                    let error = e.to_string();
                    if error.contains("already exists") {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Ok(ClickhouseLockHandle { lock: Some(self) })
    }

    /// Resets this lock, forcefully unlocking it
    pub async fn reset(&self) -> Result<(), KlickhouseError> {
        self.client
            .execute(format!(
                "DROP TABLE IF EXISTS _lock_{}{} SYNC",
                self.name, self.cluster_str
            ))
            .await
    }
}

impl ClickhouseLockHandle<'_> {
    /// Unlocks this handle (without spawning a tokio task)
    pub async fn unlock(mut self) -> Result<(), KlickhouseError> {
        self.lock.take().unwrap().reset().await
    }
}

impl Drop for ClickhouseLockHandle<'_> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take().cloned() {
            tokio::spawn(async move {
                if let Err(e) = lock.reset().await {
                    error!("failed to reset lock: {}: {e:?}", lock.name);
                }
            });
        }
    }
}
