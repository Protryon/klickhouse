use std::net::{Ipv4Addr, Ipv6Addr};

use futures::StreamExt;
use indexmap::IndexMap;
use klickhouse::{
    i256, u256, Client, ClientOptions, Date, DateTime, DateTime64, FixedPoint128, FixedPoint256,
    FixedPoint32, FixedPoint64, Ipv4, Ipv6, Uuid,
};

#[derive(klickhouse::Row, Debug, Default)]
pub struct TestType {
    d_i8: i8,
    d_i16: i16,
    d_i32: i32,
    d_i64: i64,
    d_i128: i128,
    d_i256: i256,
    d_u8: u8,
    d_u16: u16,
    d_u32: u32,
    d_u64: u64,
    // d_u128: u128,
    d_u256: u256,
    d_f32: f32,
    d_f64: f64,
    d_d32: FixedPoint32<5>,
    d_d64: FixedPoint64<5>,
    d_d128: FixedPoint128<5>,
    d_d256: FixedPoint256<5>,
    d_string: String,
    d_fstring: String,
    d_uuid: Uuid,
    d_date: Date,
    d_datetime: DateTime,
    d_datetime64: DateTime64<3>,
    d_array: Vec<u32>,
    d_2array: Vec<Vec<u32>>,
    #[klickhouse(rename = "d_nested.id")]
    d_nested_id: Vec<u32>,
    #[klickhouse(rename = "d_nested.name")]
    d_nested_name: Vec<String>,
    d_tuple: (u32, u32),
    d_nullable: Option<u32>,
    d_map: IndexMap<String, String>,
    d_null_string: Option<String>,
    d_low_card_string: String,
    d_low_card_array: Vec<String>,
    d_array_nulls: Vec<Option<String>>,
    d_low_card_array_nulls: Vec<Option<String>>,
    d_ip4: Ipv4,
    d_ip6: Ipv6,
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
        .query::<TestType>("select * from test_types;")
        .await
        .unwrap();
    while let Some(name) = names.next().await {
        let name = name.unwrap();
        println!("name = {:?}", name);
    }

    println!("begin insert");

    let mut block = TestType::default();
    block
        .d_low_card_array
        .push("te1ssdsdsdsdasdasdasdsadt".to_string());
    block.d_low_card_array.push("te2st".to_string());
    block.d_ip4 = "5.6.7.8".parse::<Ipv4Addr>().unwrap().into();
    block.d_ip6 = "ff26:0:0:0:0:0:0:c5".parse::<Ipv6Addr>().unwrap().into();

    client
        .insert_native_block("insert into test_types format native", vec![block])
        .await
        .unwrap();

    println!("done");
}
