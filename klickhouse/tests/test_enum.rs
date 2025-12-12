use indexmap::IndexMap;
use klickhouse::block::Block;
use klickhouse::{Type, Value};
use tokio_stream::StreamExt;

#[tokio::test]
async fn test_client() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table(
        "test_enum",
        " e8 Enum8('ONE' = 1, 'TWO' = 2), e16 Enum16('THREE' = 3, 'FOUR' = 4)",
        &client,
    )
    .await;

    let column_types = IndexMap::from([
        (
            "e8".to_string(),
            Type::Enum8(vec![("ONE".to_string(), 1), ("TWO".to_string(), 2)]),
        ),
        (
            "e16".to_string(),
            Type::Enum16(vec![("THREE".to_string(), 3), ("FOUR".to_string(), 4)]),
        ),
    ]);

    let column_data = IndexMap::from([
        ("e8".to_string(), vec![Value::Enum8(1), Value::Enum8(2)]),
        ("e16".to_string(), vec![Value::Enum16(3), Value::Enum16(4)]),
    ]);
    let inserted_block = Block {
        info: Default::default(),
        rows: 2,
        column_types,
        column_data,
    };

    let mut insert_res = client
        .insert_native_raw(
            "INSERT INTO test_enum FORMAT Native",
            futures_util::stream::iter([inserted_block.clone()]),
        )
        .await
        .unwrap();

    let first_block = insert_res.next().await.unwrap().unwrap();
    assert_eq!(first_block.column_types, inserted_block.column_types);

    while let Some(block) = insert_res.next().await {
        println!("insert: {:?}", block);
        block.unwrap();
    }

    let mut result = client.query_raw("SELECT * FROM test_enum").await.unwrap();

    let first_block = result.next().await.unwrap().unwrap();
    let second_block = result.next().await.unwrap().unwrap();
    let third_block = result.next().await.unwrap().unwrap();

    println!("first_block: {:?}", first_block);
    assert_eq!(first_block.column_types, inserted_block.column_types);
    assert_eq!(first_block.column_types, second_block.column_types);

    println!("second_block: {:?}", second_block);
    assert_eq!(second_block.rows, 2);
    assert_eq!(second_block.column_data, inserted_block.column_data);

    println!("third_block: {:?}", third_block);
    assert_eq!(third_block.rows, 0);
    assert!(third_block.column_types.is_empty());

    assert!(result.next().await.is_none());
}
