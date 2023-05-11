use std::time::Duration;

use klickhouse::{ClickhouseLock, Client, ClientOptions};

#[tokio::test]
async fn test_client() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let client1 = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();
    let client2 = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    let lock1 = ClickhouseLock::new(client1.clone(), "test");
    let handle = lock1.lock().await.unwrap();
    println!("lock1 locked");

    let lock2 = ClickhouseLock::new(client2.clone(), "test");
    match tokio::time::timeout(Duration::from_secs(1), lock2.lock()).await {
        Ok(_) => panic!("failed test"),
        Err(_) => (), // passed
    }

    println!("lock1 unlocking");
    tokio::time::sleep(Duration::from_secs(1)).await;
    handle.unlock().await.unwrap();
    println!("lock1 unlocked");
    println!("lock2 locking...");
    let handle2 = lock2.lock().await.unwrap();
    println!("lock2 locked");
    handle2.unlock().await.unwrap();
}
