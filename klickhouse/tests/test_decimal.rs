#![cfg(feature = "rust_decimal")]
use rust_decimal::Decimal;

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct TestType {
    d_d128: Decimal,
}

#[tokio::test]
async fn test_client() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table("test_decimal", "d_d128 Decimal128(5) default 0", &client).await;

    println!("begin insert");

    let block = TestType {
        d_d128: Decimal::new(12345, 2),
    };

    client
        .insert_native_block(
            "INSERT INTO test_decimal (d_d128) FORMAT Native",
            vec![block.clone()],
        )
        .await
        .unwrap();

    let block2 = client
        .query_one("SELECT * from test_decimal")
        .await
        .unwrap();
    assert_eq!(block, block2);

    println!("done");
}
