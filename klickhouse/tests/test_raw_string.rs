use klickhouse::Bytes;

#[derive(klickhouse::Row, Debug, Default, Clone, PartialEq, Eq)]
pub struct TestRawString {
    raw_string: Bytes,
}

#[tokio::test]
async fn test_raw_string() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let client = super::get_client().await;

    client
        .execute("drop table if exists test_raw_string")
        .await
        .unwrap();
    client
        .execute(
            r"
    CREATE TABLE test_raw_string (
        raw_string String
    ) ENGINE = Memory;
    ",
        )
        .await
        .unwrap();

    println!("begin insert");

    let items = vec![TestRawString {
        raw_string: Bytes(vec![0x0, 0x20, 0x8c, 0x5d, 0x9f]),
    }];

    client
        .insert_native_block("insert into test_raw_string format native", items.clone())
        .await
        .unwrap();

    println!("inserted rows:\n{items:#?}\n\n\n\n\n");

    let rows_back = client
        .query_collect::<TestRawString>("select * from test_raw_string")
        .await
        .unwrap();

    for row in &rows_back {
        println!("row received {row:#?}");
    }
    assert_eq!(items, rows_back)
}
