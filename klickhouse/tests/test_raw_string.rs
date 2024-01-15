use klickhouse::Bytes;

#[derive(klickhouse::Row, Debug, Default, Clone, PartialEq, Eq)]
pub struct TestRawString {
    raw_string: Bytes,
}

#[tokio::test]
async fn test_raw_string() {
    let client = super::get_client().await;

    super::prepare_table("test_raw_string", "raw_string String", &client).await;

    println!("begin insert");

    let items = vec![TestRawString {
        raw_string: Bytes(vec![0x0, 0x20, 0x8c, 0x5d, 0x9f]),
    }];

    client
        .insert_native_block("INSERT INTO test_raw_string FORMAT NATIVE", items.clone())
        .await
        .unwrap();

    println!("inserted rows:\n{items:#?}\n\n\n\n\n");

    let rows_back = client
        .query_collect::<TestRawString>("SELECT * FROM test_raw_string")
        .await
        .unwrap();

    for row in &rows_back {
        println!("row received {row:#?}");
    }
    assert_eq!(items, rows_back)
}
