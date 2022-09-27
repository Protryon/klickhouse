use chrono::Utc;
use futures::StreamExt;
use klickhouse::*;

/*
create table my_user_data (
    id UUID,
    user_data String,
    created_at DateTime('UTC')
) Engine=Memory;
*/
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
    let client = Client::connect(
        "54.202.35.255:9000",
        ClientOptions {
            username: "root".to_string(),
            password: "UPkaZzAeSJYIL557".to_string(),
            default_database: "infra_data".to_string(),
        },
    )
    .await
    .unwrap();
    /*
    host: 54.202.35.255
    port: 9000
    name: infra_data
    user: root
    password: UPkaZzAeSJYIL557

          */

    let mut row = MyUserData::default();
    row.id = Uuid::new_v4();
    row.user_data = "some important stuff!".to_string();
    row.created_at = Utc::now().try_into().unwrap();

    client
        .insert_native_block("INSERT INTO my_user_data FORMAT native", vec![row])
        .await
        .unwrap();

    let mut all_rows = client
        .query::<MyUserData>("select * from my_user_data;")
        .await
        .unwrap();

    while let Some(row) = all_rows.next().await {
        let row = row.unwrap();
        println!("row received '{}': {:?}", row.id, row);
    }
}
