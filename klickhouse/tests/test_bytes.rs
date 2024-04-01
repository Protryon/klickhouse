use klickhouse::{Bytes, RawRow};

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct Row {
    raw_bytes: Vec<u8>,
    raw_bytes2: Bytes,
    raw_bytes_fixed: Vec<u8>,
    raw_bytes_fixed2: Bytes,
    raw_bytes_arr: Vec<u8>,
    raw_bytes_arr2: Bytes,
    raw_bytes_arrs: Vec<u8>,
    raw_bytes_arrs2: Bytes,
}

#[tokio::test]
#[allow(clippy::field_reassign_with_default)]
async fn test_client() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table(
        "test_bytes",
        r"
        raw_bytes String,
        raw_bytes2 String,
        raw_bytes_fixed FixedString(8),
        raw_bytes_fixed2 FixedString(8),
        raw_bytes_arr Array(UInt8),
        raw_bytes_arr2 Array(UInt8),
        raw_bytes_arrs Array(Int8),
        raw_bytes_arrs2 Array(Int8)
    ",
        &client,
    )
    .await;

    println!("begin insert");

    let mut items = Vec::with_capacity(2);

    let raw_bytes = vec![b'B', 0, 255, 128, 127, b'A'];
    let raw_bytes2: Bytes = b"test_bytes".to_vec().into();

    for _ in 0..items.capacity() {
        let item = Row {
            raw_bytes: raw_bytes.clone(),
            raw_bytes2: raw_bytes2.clone(),
            raw_bytes_fixed: raw_bytes.clone(),
            raw_bytes_fixed2: raw_bytes2.clone(),
            raw_bytes_arr: raw_bytes.clone(),
            raw_bytes_arr2: raw_bytes2.clone(),
            raw_bytes_arrs: raw_bytes.clone(),
            raw_bytes_arrs2: raw_bytes2.clone(),
        };

        items.push(item);
    }

    client
        .insert_native_block("INSERT INTO test_bytes FORMAT Native", items.clone())
        .await
        .unwrap();

    println!("done");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let items2 = client
        // TODO: Why can't we deserialize back into Row? raw_bytes tries to get deserialized into a
        // String directly.
        .query_collect::<RawRow>("SELECT * FROM test_bytes")
        .await
        .unwrap();
    println!("{:?}", items2);
}
