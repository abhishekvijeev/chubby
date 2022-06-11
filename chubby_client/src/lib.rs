#[cfg(test)]
mod tests {
    use super::*;
    use chubby_server::constants::LockMode;
    use std::error::Error;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use std::time::{Duration, Instant};

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

    #[tokio::test()]
    async fn test_client_acquire_lock() -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let mut client = client::ChubbyClient::new().await?;
        client.create_session(true).await?;
        let path = "/lock1";
        println!("sending open");
        client.open(String::from(path)).await?;
        client.acquire(String::from(path), LockMode::SHARED).await?;
        client.release(String::from(path)).await?;
        client.acquire(String::from(path), LockMode::EXCLUSIVE).await?;
        client.release(String::from(path)).await?;
        Ok(())
    }

    async fn acquire_lock(client_arc: Arc<Mutex<client::ChubbyClient>>, path_arc: Arc<Mutex<String>>) {
        let mut client = client_arc.lock().await;
        let path = path_arc.lock().await;
        client.acquire(path.clone(), LockMode::EXCLUSIVE).await;
    }

    #[tokio::test()]
    async fn test_locks_per_unit_time() -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let mut clients = Vec::new();
        let mut lock_paths = Vec::new();
        let mut thread_handles = Vec::new();
        let num_locks = 3000;

        for i in 1..num_locks {
            let mut client = client::ChubbyClient::new().await?;
            let path = format!("{}{}", "/lock/n", i);
            client.create_session(true).await?;
            client.open(path.clone()).await?;
            clients.push(Arc::new(Mutex::new(client)));
            lock_paths.push(Arc::new(Mutex::new(path)));
        }

        let begin = Instant::now();
        for i in 0..(num_locks-1) {
            let shared_client = clients[i].clone();
            let shared_path = lock_paths[i].clone();
            thread_handles.push(tokio::spawn(async move {
                acquire_lock(shared_client, shared_path).await;
            }));
        }
        while thread_handles.len() > 0 {
            let h = thread_handles.remove(0);
            h.await;
        }
        println!("Time to acquire {} locks: {}ms", num_locks, begin.elapsed().as_millis());
        Ok(())
    }
}
pub mod client;
mod constants;
mod err;
