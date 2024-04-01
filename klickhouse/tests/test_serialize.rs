use std::collections::HashMap;

use chrono::Utc;
use klickhouse::{DateTime64, Uuid};

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct TestSerialize {
    d_uuid: Uuid,
    d_date: DateTime64<6>,
    d_u64: u64,
    d_i32: i32,
    d_i16: i16,
    d_map_null_string: HashMap<String, Option<String>>,
    d_bool: bool,
    d_string: String,

    #[klickhouse(rename = "nest.nest_string")]
    nest_string: Vec<String>,
    #[klickhouse(rename = "nest.nest_u64")]
    nest_u64: Vec<Option<u64>>,
    #[klickhouse(rename = "nest.nest_null_string")]
    nest_null_string: Vec<Option<String>>,
    #[klickhouse(rename = "nest.nest_i16")]
    nest_i16: Vec<i16>,

    d_map_u64: HashMap<String, u64>,
    d_map_string: HashMap<String, String>,
    d_null_string: Option<String>,
    d_vec: Vec<String>,
    d_vec2: Vec<Option<String>>,
}

#[tokio::test]
async fn test_client() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table(
        "test_serialize",
        r"
        d_uuid UUID,
        d_date DateTime64(6),
        d_u64 UInt64,
        d_i32 Int32,
        d_i16 Int16,
        d_map_null_string Map(String, Nullable(String)),
        d_bool Bool,
        d_string String,

        nest Nested
        (
            nest_string String,
            nest_u64 Nullable(UInt64),
            nest_null_string Nullable(String),
            nest_i16 Int16
        ),
        d_map_u64 Map(String, UInt64),
        d_map_string Map(String, String),
        d_null_string Nullable(String),
        d_vec Array(String),
        d_vec2 Array(Nullable(String)),
    ",
        &client,
    )
    .await;

    println!("begin insert");

    let mut items = Vec::with_capacity(2);

    for i in 0..items.capacity() {
        let item = TestSerialize {
            d_i16: 16,
            d_i32: 32,
            d_u64: 64,
            d_uuid: Uuid::new_v4(),
            d_date: Utc::now().try_into().unwrap(),
            d_bool: true,
            d_vec: vec![format!("test{i}"), format!("test{}", i + 10)],
            d_vec2: vec![
                Some(format!("test{}", -(i as isize))),
                Some(format!("test{}", -(i as isize) - 10)),
            ],
            d_string: "testn".to_string(),
            d_null_string: Some("test_string".to_string()),
            d_map_null_string: HashMap::from([
                ("test2".to_string(), None),
                ("test3".to_string(), Some("test3_value".to_string())),
                ("test1".to_string(), Some("test1_value".to_string())),
            ]),
            d_map_u64: HashMap::from([("mapu64_1".into(), 5), ("mapu64_1".into(), 4)]),

            d_map_string: HashMap::from([
                ("test4".into(), "test4_value".into()),
                ("test5".into(), "test5_value".into()),
            ]),

            nest_u64: vec![Some(1), None, Some(2)],
            nest_i16: vec![1, 2, 3],
            nest_string: vec![
                "nest1_string".into(),
                "nest2_string".into(),
                "nest3_string".into(),
            ],
            nest_null_string: vec![
                Some("nest1_nstring".to_string()),
                None,
                Some("nest3_nstring".to_string()),
            ],
        };

        items.push(item);
    }

    client
        .insert_native_block("INSERT INTO test_serialize FORMAT NATIVE", items.clone())
        .await
        .unwrap();

    println!("done");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let items2 = client
        .query_collect::<TestSerialize>("SELECT * FROM test_serialize")
        .await
        .unwrap();
    assert_eq!(items, items2);
}
