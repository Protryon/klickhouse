#[derive(klickhouse::Row, Debug, PartialEq, Clone)]
struct TestRow {
    b: klickhouse::Bytes,
    a: klickhouse::Bytes,
}

// This test checks that the order of declaration in the struct does not matter for type hinting.
// See https://github.com/Protryon/klickhouse/issues/34
#[tokio::test]
async fn ordering() {
    let client = super::get_client().await;

    super::prepare_table(
        "test_ordering",
        "a Array(UInt8),
         b String",
        &client,
    )
    .await;

    let row = TestRow {
        a: vec![1, 2, 3].into(),
        b: vec![4, 5, 6].into(),
    };
    client
        .insert_native_block("INSERT INTO test_ordering FORMAT Native", vec![row.clone()])
        .await
        .unwrap();

    let row2 = client
        .query_one("SELECT * from test_ordering")
        .await
        .unwrap();
    assert_eq!(row, row2);
}
