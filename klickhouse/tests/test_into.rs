#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
#[klickhouse(into = "TestType2")]
pub struct TestType1 {
    d_i8: i8,
}

impl Into<TestType2> for TestType1 {
    fn into(self) -> TestType2 {
        TestType2 {
            d_i16: self.d_i8 as i16,
        }
    }
}

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct TestType2 {
    d_i16: i16,
}

#[tokio::test]
async fn test_client() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table(
        "test_into",
        r"
        d_i16 Int16 default 0,
    ",
        &client,
    )
    .await;

    println!("begin insert");

    let block = TestType2::default();
    client
        .insert_native_block("insert into test_into format native", vec![block.clone()])
        .await
        .unwrap();

    let block2 = client.query_one("SELECT * from test_into").await.unwrap();
    assert_eq!(block, block2);

    println!("done");
}
