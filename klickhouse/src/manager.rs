use std::net::SocketAddr;

use futures::StreamExt;

use crate::{convert::UnitValue, Client, ClientOptions, KlickhouseError};

#[derive(Clone)]
pub struct ConnectionManager {
    destination: Vec<SocketAddr>,
    options: ClientOptions,
}

#[async_trait::async_trait]
impl bb8::ManageConnection for ConnectionManager {
    type Connection = Client;
    type Error = KlickhouseError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Client::connect(&self.destination[..], self.options.clone())
            .await
            .map_err(|e| KlickhouseError::Io(e))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let mut stream = conn.query::<UnitValue<String>>("select '';").await?;
        stream.next().await.ok_or_else(|| {
            KlickhouseError::DeserializeError("invalid ping response".to_string())
        })?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
    }
}
