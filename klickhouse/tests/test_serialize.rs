use std::collections::HashMap;

use chrono::Utc;
use klickhouse::{Client, ClientOptions, DateTime64, RawRow, Uuid};

#[derive(klickhouse::Row, Debug, Default)]
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

#[derive(klickhouse::Row, Debug, Default)]
pub struct Nest {
    nest_string: String,
    nest_u64: Option<u64>,
    nest_null_string: Option<String>,
    nest_i16: i16,
}

#[derive(klickhouse::Row, Debug, Default)]
pub struct TestSerializeNested {
    #[klickhouse(nested)]
    nest: Vec<Nest>,
}

// used to test compilation with multiple nested entries
#[allow(unused)]
#[derive(klickhouse::Row, Debug, Default)]
struct TestSerializeNested2 {
    #[klickhouse(nested)]
    nest: Vec<Nest>,
    #[klickhouse(nested)]
    nest2: Vec<Nest>,
}

#[tokio::test]
async fn test_client() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    client.execute("truncate test_serialize").await.unwrap();

    println!("begin insert");

    let mut items = Vec::with_capacity(2);
    for i in 0..items.capacity() {
        let mut item = TestSerialize::default();
        item.d_uuid = Uuid::new_v4();
        item.d_date = Utc::now().try_into().unwrap();
        item.d_map_null_string
            .insert("test1".to_string(), Some("test1_value".to_string()));
        item.d_map_null_string.insert("test2".to_string(), None);
        item.d_map_null_string
            .insert("test3".to_string(), Some("test3_value".to_string()));
        item.d_bool = true;
        item.d_string = "testn".to_string();
        item.nest_string.push("nest1_string".to_string());
        item.nest_string.push("nest2_string".to_string());
        item.nest_string.push("nest3_string".to_string());
        item.nest_u64.push(Some(1));
        item.nest_u64.push(None);
        item.nest_u64.push(Some(2));
        item.nest_null_string
            .push(Some("nest1_nstring".to_string()));
        item.nest_null_string.push(None);
        item.nest_null_string
            .push(Some("nest3_nstring".to_string()));
        item.nest_i16.push(1);
        item.nest_i16.push(2);
        item.nest_i16.push(3);
        item.d_map_u64.insert("mapu64_1".to_string(), 5);
        item.d_map_u64.insert("mapu64_1".to_string(), 4);
        item.d_map_string
            .insert("test4".to_string(), "test4_value".to_string());
        item.d_map_string
            .insert("test5".to_string(), "test5_value".to_string());
        item.d_null_string = Some("test_string".to_string());

        item.d_vec.push(format!("test{i}"));
        item.d_vec.push(format!("test{}", i + 10));
        item.d_vec2.push(Some(format!("test{}", -(i as isize))));
        item.d_vec2
            .push(Some(format!("test{}", -(i as isize) - 10)));
        items.push(item);
    }

    client
        .insert_native_block("insert into test_serialize format native", items)
        .await
        .unwrap();

    println!("done");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    for row in client
        .query_collect::<RawRow>("select * from test_serialize")
        .await
        .unwrap()
    {
        println!("row {row:#?}");
    }

    let nested = TestSerializeNested {
        nest: vec![
            Nest {
                nest_string: "nest1".to_string(),
                nest_u64: Some(1),
                nest_null_string: None,
                nest_i16: 32,
            },
            Nest {
                nest_string: "nest2".to_string(),
                nest_u64: Some(2),
                nest_null_string: None,
                nest_i16: 64,
            },
        ],
    };

    client.execute("TRUNCATE test_serialize").await.unwrap();

    println!("reinserting");
    client
        .insert_native_block(
            "insert into test_serialize format native",
            vec![nested, TestSerializeNested::default()],
        )
        .await
        .unwrap();
    println!("reinserted");

    for row in client
        .query_collect::<TestSerializeNested>("select nest.nest_string, nest.nest_u64, nest.nest_null_string, nest.nest_i16 from test_serialize")
        .await
        .unwrap()
    {
        println!("row_nested {row:#?}");
    }
}
