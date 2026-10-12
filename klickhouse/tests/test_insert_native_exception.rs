use futures_util::StreamExt;
use klickhouse::block::{Block, BlockInfo};
use klickhouse::{Client, IndexMap, KlickhouseError, Type, UnitValue, Value};

#[derive(klickhouse::Row, Debug)]
struct ConstraintRow {
    id: u32,
}

fn init_logger() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
}

fn assert_server_exception(result: std::result::Result<(), KlickhouseError>, context: &str) {
    assert!(
        matches!(result, Err(KlickhouseError::ServerException { .. })),
        "{context} must propagate ClickHouse errors after data blocks are sent, got {result:?}"
    );
}

async fn prepare_constraint_table(client: &Client, table_name: &str) {
    client
        .execute(format!("DROP TABLE IF EXISTS {table_name}"))
        .await
        .unwrap();
    client
        .execute(format!(
            "CREATE TABLE {table_name} (
                id UInt32,
                CONSTRAINT id_at_least_100 CHECK id >= 100
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .await
        .unwrap();
}

/// ClickHouse rejects this row only after the native data block is received and
/// validated server-side. A plain SQL insert confirms the server would fail.
async fn assert_clickhouse_rejects_row(client: &Client, table_name: &str) {
    let sql_result = client
        .execute(format!("INSERT INTO {table_name} VALUES (1)"))
        .await;
    assert!(
        sql_result.is_err(),
        "expected ClickHouse to reject id=1 via SQL insert"
    );
}

fn violating_row_block() -> Block {
    let mut column_types = IndexMap::new();
    column_types.insert("id".to_string(), Type::UInt32);
    let mut column_data = IndexMap::new();
    column_data.insert("id".to_string(), vec![Value::UInt32(1)]);

    Block {
        info: BlockInfo::default(),
        rows: 1,
        column_types,
        column_data,
    }
}

/// `insert_native` must not return success before observing the final server
/// response. Otherwise post-data exceptions are dropped when the query receiver
/// is closed.
#[tokio::test]
async fn insert_native_block_returns_server_error_after_data_sent() {
    init_logger();
    let client = super::get_client().await;
    let table_name = "test_insert_native_exception_block";

    prepare_constraint_table(&client, table_name).await;
    assert_clickhouse_rejects_row(&client, table_name).await;

    let result = client
        .insert_native_block(
            format!("INSERT INTO {table_name} FORMAT Native"),
            vec![ConstraintRow { id: 1 }],
        )
        .await;

    assert_server_exception(result, "insert_native_block");

    let count = client
        .query_one::<UnitValue<u64>>(format!("SELECT count() FROM {table_name}"))
        .await
        .unwrap()
        .0;
    assert_eq!(count, 0);
}

/// The streaming API must also wait for ClickHouse's final response after all
/// outgoing blocks have been sent, not just the single-block wrapper path.
#[tokio::test]
async fn insert_native_stream_returns_server_error_after_multiple_data_blocks_sent() {
    init_logger();
    let client = super::get_client().await;
    let table_name = "test_insert_native_exception_stream";

    prepare_constraint_table(&client, table_name).await;
    assert_clickhouse_rejects_row(&client, table_name).await;

    let result = client
        .insert_native(
            format!("INSERT INTO {table_name} FORMAT Native"),
            futures_util::stream::iter(vec![
                vec![ConstraintRow { id: 100 }],
                vec![ConstraintRow { id: 1 }],
            ]),
        )
        .await;

    assert_server_exception(result, "insert_native");

    let invalid_count = client
        .query_one::<UnitValue<u64>>(format!("SELECT count() FROM {table_name} WHERE id < 100"))
        .await
        .unwrap()
        .0;
    assert_eq!(invalid_count, 0);
}

/// The same failing insert is observable when the caller drains `insert_native_raw`.
#[tokio::test]
async fn insert_native_raw_observes_server_exception_when_drained() {
    init_logger();
    let client = super::get_client().await;
    let table_name = "test_insert_native_exception_raw";

    prepare_constraint_table(&client, table_name).await;
    assert_clickhouse_rejects_row(&client, table_name).await;

    let mut stream = client
        .insert_native_raw(
            format!("INSERT INTO {table_name} FORMAT Native"),
            futures_util::stream::iter([violating_row_block()]),
        )
        .await
        .unwrap();

    let mut saw_server_exception = false;
    while let Some(item) = stream.next().await {
        if matches!(item, Err(KlickhouseError::ServerException { .. })) {
            saw_server_exception = true;
            break;
        }
    }

    assert!(
        saw_server_exception,
        "draining insert_native_raw should observe the server exception"
    );
}
