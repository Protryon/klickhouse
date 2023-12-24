use klickhouse::{MultiPolygon, Point, Polygon, Ring};

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct Row {
    point: Point,
    ring: Ring,
    polygon: Polygon,
    multipolygon: MultiPolygon,
}

#[tokio::test]
#[allow(clippy::field_reassign_with_default)]
async fn test_client() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let client = super::get_client().await;

    super::prepare_table(
        "test_geo",
        r"
        point Point,
        ring Ring,
        polygon Polygon,
        multipolygon MultiPolygon",
        &client,
    )
    .await;

    println!("begin insert");

    let mut items = Vec::with_capacity(2);

    for i in 0..items.capacity() {
        let i = i as f64;
        let ring = Ring(vec![Point([i, i + 2.0]), Point([i + 3.0, i + 4.0])]);
        let polygon = |j| {
            Polygon(vec![
                ring.clone(),
                Ring(vec![
                    Point([j + i + 5.0, j + i + 6.0]),
                    Point([j + i + 7.0, j + i + 8.0]),
                ]),
            ])
        };
        let item = Row {
            point: Point([i, i + 2.0]),
            ring: ring.clone(),
            polygon: polygon(0.0),
            multipolygon: MultiPolygon(vec![polygon(0.0), polygon(10.0)]),
        };

        items.push(item);
    }

    client
        .insert_native_block("INSERT INTO test_geo FORMAT Native", items.clone())
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let items2 = client
        // String directly.
        .query_collect::<Row>("SELECT * FROM test_geo")
        .await
        .unwrap();
    assert_eq!(items, items2);
}
