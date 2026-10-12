use klickhouse::{LocalMigration, UnitValue};
use refinery_core::{Migration, Runner};

#[derive(klickhouse::Row, Debug, PartialEq)]
struct TestMigrateRow {
    id: u32,
    name: String,
}

#[tokio::test]
async fn test_local_migration() {
    let client = super::get_client().await;
    let database = client
        .query_one::<UnitValue<String>>("SELECT currentDatabase()")
        .await
        .unwrap()
        .0;

    for table in ["test_migrate", "test_migrate_history"] {
        client
            .execute(format!("DROP TABLE IF EXISTS {table}"))
            .await
            .unwrap();
    }

    // Same migrations as would be run with `ClusterMigration`.
    let migrations = [
        Migration::unapplied(
            "V1__create",
            "CREATE TABLE %DATABASE%.test_migrate %ON_CLUSTER% (id UInt32) ENGINE = MergeTree() ORDER BY id;",
        )
        .unwrap(),
        Migration::unapplied(
            "V2__alter",
            "ALTER TABLE %DATABASE%.test_migrate %ON_CLUSTER% ADD COLUMN name String;
            INSERT INTO %DATABASE%.test_migrate VALUES (1, 'one');",
        )
        .unwrap(),
    ];
    let mut runner = Runner::new(&migrations);
    runner.set_migration_table_name("test_migrate_history");

    let mut migration = LocalMigration::new(client.clone(), database.clone());
    let report = runner.run_async(&mut migration).await.unwrap();
    assert_eq!(report.applied_migrations().len(), 2);

    // Re-running is a no-op.
    let mut migration = LocalMigration::new(client.clone(), database).without_lock();
    let report = runner.run_async(&mut migration).await.unwrap();
    assert!(report.applied_migrations().is_empty());

    let rows = client
        .query_collect::<TestMigrateRow>("SELECT id, name FROM test_migrate")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![TestMigrateRow {
            id: 1,
            name: "one".to_string()
        }]
    );
}
