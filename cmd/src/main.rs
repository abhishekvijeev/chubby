use chubby_client::client::ChubbyClient;
// use tokio   ;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ChubbyClient::new().await?;
    client.create_session().await;
    client.delete_session().await;
    // println!("Hello, world!");
    Ok(())
}
