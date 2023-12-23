pub mod test;
pub mod test_decimal;
pub mod test_lock;
pub mod test_raw_string;
pub mod test_serialize;

use klickhouse::{Client, ClientOptions};

pub async fn get_client() -> Client {
    let address = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    Client::connect(address, ClientOptions::default())
        .await
        .unwrap()
}
