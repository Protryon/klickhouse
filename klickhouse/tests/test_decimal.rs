#![cfg(feature = "rust_decimal")]
use futures::StreamExt;
use klickhouse::{Client, ClientOptions};
use rust_decimal::Decimal;

#[derive(klickhouse::Row, Debug, Default)]
pub struct TestType {
    d_d128: Decimal,
}

#[tokio::test]
async fn test_client() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();
    let mut names = client
        .query::<TestType>("select d_d128 from test_types;")
        .await
        .unwrap();
    while let Some(name) = names.next().await {
        let name = name.unwrap();
        println!("d_d128 = {:?}", name);
    }

    println!("begin insert");

    let mut block = TestType::default();
    block.d_d128 = Decimal::new(12345, 2);

    client
        .insert_native_block("insert into test_types (d_d128) format native", vec![block])
        .await
        .unwrap();

    println!("done");
}
