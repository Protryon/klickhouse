use indexmap::IndexMap;
use klickhouse::RawRow;
use tokio_stream::StreamExt;

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const TRACESTATE: &str = "vendor=test";

#[derive(klickhouse::Row, Debug, Clone, PartialEq)]
struct TestRow {
    n: u8,
}

#[tokio::test]
async fn test_settings_default() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let mut opts = klickhouse::ClientOptions::default();
    if let Ok(u) = std::env::var("KLICKHOUSE_TEST_USER") {
        opts.username = u;
    }
    if let Ok(p) = std::env::var("KLICKHOUSE_TEST_PASSWORD") {
        opts.password = p;
    }
    if let Ok(db) = std::env::var("KLICKHOUSE_TEST_DATABASE") {
        opts.default_database = db;
    }
    opts.settings
        .insert("opentelemetry_traceparent".into(), TRACEPARENT.into());
    opts.settings
        .insert("opentelemetry_tracestate".into(), TRACESTATE.into());
    opts.settings
        .insert("log_comment".into(), "test_settings_default".into());

    let addr = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let client = klickhouse::Client::connect(addr, opts).await.unwrap();

    // Execute with default settings
    client.execute("SELECT 1").await.unwrap();

    // Query via higher-level API inherits defaults
    let rows = client
        .query_collect::<RawRow>("SELECT number FROM system.numbers LIMIT 1")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // Per-query settings override (merged with defaults)
    let per_query: IndexMap<String, String> =
        [("log_comment".into(), "per_query_override".into())].into_iter().collect();
    let mut stream = client.query_raw("SELECT 2", Some(per_query)).await.unwrap();
    while stream.next().await.is_some() {}

    // query_raw with None uses defaults only
    let mut stream = client.query_raw("SELECT 3", None).await.unwrap();
    while stream.next().await.is_some() {}
}

#[tokio::test]
async fn test_settings_insert_readback() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let client = super::get_client().await;
    super::prepare_table("test_settings_insert", "n UInt8", &client).await;

    client
        .insert_native_block(
            "INSERT INTO test_settings_insert FORMAT Native",
            vec![TestRow { n: 1 }, TestRow { n: 2 }],
        )
        .await
        .unwrap();

    let result = client
        .query_one::<TestRow>("SELECT toUInt8(sum(n)) AS n FROM test_settings_insert")
        .await
        .unwrap();
    assert_eq!(result.n, 3);
}