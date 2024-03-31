pub mod test;
pub mod test_bytes;
pub mod test_decimal;
pub mod test_flatten;
#[cfg(feature = "geo-types")]
pub mod test_geo;
pub mod test_lock;
pub mod test_nested;
pub mod test_ordering;
pub mod test_raw_string;
pub mod test_serialize;

use klickhouse::{Client, ClientOptions};

pub async fn get_client() -> Client {
    let mut options = ClientOptions::default();

    if let Ok(user) = std::env::var("KLICKHOUSE_TEST_USER") {
        options.username = user;
    }

    if let Ok(password) = std::env::var("KLICKHOUSE_TEST_PASSWORD") {
        options.password = password;
    }

    if let Ok(database) = std::env::var("KLICKHOUSE_TEST_DATABASE") {
        options.default_database = database;
    }

    let address = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());

    Client::connect(address, options).await.unwrap()
}
/// Drop the table if it exists, and create it with the given structure.
/// Make sure to use distinct table names across tests to avoid conflicts between tests executing
/// simultaneously.
pub async fn prepare_table(table_name: &str, table_struct: &str, client: &Client) {
    client
        .execute(format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .unwrap();
    client
        .execute(format!(
            "CREATE TABLE {} ({}) ENGINE = Memory;",
            table_name, table_struct
        ))
        .await
        .unwrap();
}
