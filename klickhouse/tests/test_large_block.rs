#[derive(klickhouse::Row, Debug, Default, Clone, PartialEq, Eq)]
pub struct TestLargeBlock {
    value: String,
}

#[tokio::test]
async fn test_large_block() {
    let client = super::get_client().await;

    super::prepare_table("test_large_block", "value String", &client).await;

    // The block is above 1 MiB, so it is sent as more than one compressed block.
    let items: Vec<_> = (0..3)
        .map(|i| TestLargeBlock {
            value: i.to_string().repeat(1 << 20),
        })
        .collect();

    client
        .insert_native_block("INSERT INTO test_large_block FORMAT NATIVE", items.clone())
        .await
        .unwrap();

    let rows_back = client
        .query_collect::<TestLargeBlock>("SELECT * FROM test_large_block")
        .await
        .unwrap();

    assert_eq!(items, rows_back);
}
