use futures::StreamExt;
use klickhouse::*;
use chrono::Utc;

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
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default()).await.unwrap();

    let mut row = MyUserData::default();
    row.id = Uuid::new_v4();
    row.user_data = "some important stuff!".to_string();
    row.created_at = Utc::now().into();
    
    client
        .insert_native_block("insert into my_user_data format native", vec![row])
        .await.unwrap();

    let mut all_rows = client
        .query::<MyUserData>("select * from my_user_data;")
        .await.unwrap();
    
    while let Some(row) = all_rows.next().await {
        let row = row.unwrap();
        println!("row received '{}': {:?}", row.id, row);
    }
}