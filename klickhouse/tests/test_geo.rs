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
        .query_collect::<Row>("SELECT * FROM test_geo")
        .await
        .unwrap();
    assert_eq!(items, items2);
}

#[derive(Clone, PartialEq, Debug, klickhouse::Row)]
struct RowWkt {
    multipolygon: MultiPolygon,
}

#[cfg(feature = "geo-types")]
#[tokio::test]
async fn test_client_wkt() {
    let client = super::get_client().await;

    super::prepare_table("test_geo_wkt", "multipolygon MultiPolygon", &client).await;
    let multipolygon: geo_types::MultiPolygon = geo_types::wkt! {
        // Example from https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
        MULTIPOLYGON (((40.0 40.0, 20.0 45.0, 45.0 30.0, 40.0 40.0)),
                      ((20.0 35.0, 10.0 30.0, 10.0 10.0, 30.0 5.0, 45.0 20.0, 20.0 35.0),
                       (30.0 20.0, 20.0 15.0, 20.0 25.0, 30. 20.0)))
    };
    let row = RowWkt {
        multipolygon: MultiPolygon::from(multipolygon),
    };

    client
        .insert_native_block("INSERT INTO test_geo_wkt FORMAT Native", vec![row.clone()])
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let row2 = client
        .query_one::<RowWkt>("SELECT * FROM test_geo_wkt")
        .await
        .unwrap();
    assert_eq!(row2, row);
}
