#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    // Tests whether session creation and deletion succeed
    #[tokio::test()]
    async fn test_session_create_delete() -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let mut client = client::ChubbyClient::new().await?;
        client.create_session(true).await?;
        client.delete_session().await?;
        Ok(())
    }

    // Tests whether the server is able to handle multiple sessions
    #[tokio::test()]
    async fn test_multiple_client_create() -> Result<(), Box<(dyn Error + Send + Sync)>> {
        for _ in 1..1000 {
            let mut client = client::ChubbyClient::new().await?;
            client.create_session(true).await?;
        }
        Ok(())
    }

    // Tests whether the session times-out if a keep alive request
    // is not sent before the session lease expires
    #[tokio::test()]
    async fn test_client_timeout() -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let mut client = client::ChubbyClient::new().await?;
        client.create_session(false).await?;
        std::thread::sleep(std::time::Duration::from_secs(13));
        // let resp = client.open().await?;
        // if resp doesn't say session has expired, throw error
        Ok(())
    }
}
pub mod client;
mod constants;
mod err;
