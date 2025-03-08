use std::net::{Ipv4Addr, Ipv6Addr};

use half::bf16;
use indexmap::IndexMap;
use klickhouse::{
    i256, u256, Date, DateTime, DateTime64, FixedPoint128, FixedPoint256, FixedPoint32,
    FixedPoint64, Ipv4, Ipv6, Uuid,
};

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
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
    d_bf16: bf16,
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
    d_low_card_array_nulls: Vec<Option<String>>,

    d_array_nulls: Vec<Option<String>>,
    d_ip4: Ipv4,
    d_ip6: Ipv6,
}

#[tokio::test]
async fn test_client() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table("test_types", r"
        d_i8 Int8 default 0,
        d_i16 Int16 default 0,
        d_i32 Int32 default 0,
        d_i64 Int64 default 0,
        d_i128 Int128 default 0,
        d_i256 Int256 default 0,
        d_u8 UInt8 default 0,
        d_u16 UInt16 default 0,
        d_u32 UInt32 default 0,
        d_u64 UInt64 default 0,
        -- d_u128 UInt128 default 0,
        d_u256 UInt256 default 0,
        d_f32 Float32 default 0,
        d_f64 Float64 default 0,
        d_bf16 BFloat16 default 0,
        d_d32 Decimal32(5) default 0,
        d_d64 Decimal64(5) default 0,
        d_d128 Decimal128(5) default 0,
        d_d256 Decimal256(5) default 0,
        d_string String default '',
        d_fstring FixedString(16) default '',
        d_uuid UUID default '9ea35279-d562-4e3f-ae65-b2d89e7fd2fd',
        d_date Date default today(),
        d_datetime DateTime default now(),
        d_datetime64 DateTime64(3) default toDateTime64(now(), 3),
        d_array Array(UInt32) default array(1, 2, 3),
        d_2array Array(Array(UInt32)) default array(array(1, 2, 3), array(2, 3, 4), array(5, 6, 7), array(8, 9, 10)),
        d_nested Nested(id UInt32, name String),
        d_tuple Tuple(UInt32, UInt32) default tuple(1, 2),
        d_nullable Nullable(UInt32) default null,
        d_map Map(String, String) default cast((['k1', 'k2'], ['v1', 'v2']), 'Map(String, String)'),
        d_null_string Nullable(String) default null,
        d_low_card_string LowCardinality(String) default 't',
        d_low_card_array Array(LowCardinality(String)) default array('test1', 'test2'),
        d_array_nulls Array(Nullable(String)),
        d_low_card_array_nulls Array(LowCardinality(Nullable(String))),
        d_ip4 IPv4,
        d_ip6 IPv6
    ", &client).await;

    println!("begin insert");

    let block = TestType {
        d_ip4: "5.6.7.8".parse::<Ipv4Addr>().unwrap().into(),
        d_ip6: "ff26:0:0:0:0:0:0:c5".parse::<Ipv6Addr>().unwrap().into(),
        d_low_card_array: vec!["te1ssdsdsdsdasdasdasdsadt".to_string(), "te2st".to_string()],
        ..Default::default()
    };
    client
        .insert_native_block("insert into test_types format native", vec![block.clone()])
        .await
        .unwrap();

    let block2 = client.query_one("SELECT * from test_types").await.unwrap();
    assert_eq!(block, block2);

    println!("done");
}
