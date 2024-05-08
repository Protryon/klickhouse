use chrono::Utc;
use futures_util::StreamExt;
use klickhouse::*;

#[derive(Row, Debug, Default)]
pub struct MyUserData {
    id: Uuid,
    user_data: String,
    created_at: DateTime,
}

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .parse_env(env_logger::Env::default().default_filter_or("info"))
        .init();
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    // Retrieve and display query progress events
    let mut progress = client.subscribe_progress();
    let progress_task = tokio::task::spawn(async move {
        let mut current_query = Uuid::nil();
        let mut progress_total = Progress::default();
        while let Ok((query, progress)) = progress.recv().await {
            if query != current_query {
                progress_total = Progress::default();
                current_query = query;
            }
            progress_total += progress;
            println!(
                "Progress on query {}: {}/{} {:.2}%",
                query,
                progress_total.read_rows,
                progress_total.new_total_rows_to_read,
                100.0 * progress_total.read_rows as f64
                    / progress_total.new_total_rows_to_read as f64
            );
        }
    });

    // Prepare table
    client
        .execute("DROP TABLE IF EXISTS klickhouse_example")
        .await
        .unwrap();
    client
        .execute(
            "
    CREATE TABLE klickhouse_example (
         id UUID,
         user_data String,
         created_at DateTime('UTC'))
     Engine=MergeTree() ORDER BY created_at;",
        )
        .await
        .unwrap();

    // Insert rows
    let rows = (0..5)
        .map(|_| MyUserData {
            id: Uuid::new_v4(),
            user_data: "some important stuff!".to_string(),
            created_at: Utc::now().try_into().unwrap(),
        })
        .collect();
    client
        .insert_native_block("INSERT INTO klickhouse_example FORMAT native", rows)
        .await
        .unwrap();

    // Read back rows
    let mut all_rows = client
        .query::<MyUserData>("SELECT * FROM klickhouse_example;")
        .await
        .unwrap();

    while let Some(row) = all_rows.next().await {
        let row = row.unwrap();
        println!("row received '{}': {:?}", row.id, row);
    }

    // Drop the client so that the progress task finishes.
    drop(client);
    progress_task.await.unwrap();
}
