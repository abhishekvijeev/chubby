use chubby_client::client::ChubbyClient;
// use tokio   ;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ChubbyClient::new().await?;
    client.create_session().await;
    std::thread::sleep(std::time::Duration::from_secs(30));
    client.delete_session().await;

    // println!("Hello, world!");
    Ok(())
}
