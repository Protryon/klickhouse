#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct Nest {
    nest_string: String,
    nest_u64: Option<u64>,
    nest_null_string: Option<String>,
    nest_i16: i16,
}

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
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
    let client = super::get_client().await;

    super::prepare_table(
        "test_nested",
        r"
        nest Nested
        (
            nest_string String,
            nest_u64 Nullable(UInt64),
            nest_null_string Nullable(String),
            nest_i16 Int16
        ),
    ",
        &client,
    )
    .await;

    let items = vec![
        TestSerializeNested {
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
        },
        TestSerializeNested::default(),
    ];

    client
        .insert_native_block("INSERT INTO test_nested FORMAT Native", items.clone())
        .await
        .unwrap();

    let items2= client
        .query_collect::<TestSerializeNested>("SELECT nest.nest_string, nest.nest_u64, nest.nest_null_string, nest.nest_i16 FROM test_nested")
        .await
        .unwrap();
    assert_eq!(items, items2);
}
