use klickhouse::Row;

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct TestRow {
    field: u32,
    #[klickhouse(flatten)]
    subrow: SubRow,
    field2: u32,
}

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct SubRow {
    a: u32,
    b: f32,
}

#[tokio::test]
async fn test_client() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    assert!(TestRow::column_names()
        .unwrap()
        .into_iter()
        .zip(["field", "a", "b"])
        .all(|(x, y)| x == y));

    let client = super::get_client().await;

    super::prepare_table(
        "test_flatten",
        "field UInt32,
         field2 UInt32,
         a UInt32,
         b Float32",
        &client,
    )
    .await;

    let row = TestRow {
        field: 1,
        field2: 4,
        subrow: SubRow { a: 2, b: 3.0 },
    };

    client
        .insert_native_block("INSERT INTO test_flatten FORMAT Native", vec![row.clone()])
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let row2 = client
        .query_one::<TestRow>("SELECT * FROM test_flatten")
        .await
        .unwrap();
    assert_eq!(row, row2);
}
