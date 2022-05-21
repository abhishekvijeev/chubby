use crate::err::ChubbyClientError;
use chubby_server::rpc;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::{thread, time};
use tokio;
use tokio::sync::oneshot;
use tonic::transport::channel::Channel;

pub struct ChubbyClient {
    session: Arc<tokio::sync::Mutex<Option<ChubbyClientSession>>>,
}

#[derive(Debug, Clone)]
struct ChubbyClientSession {
    session_id: String,
    lease_length: Duration,
    conn: rpc::chubby_client::ChubbyClient<Channel>,
}

// async fn send_keep_alive(session: &mut Option<ChubbyClientSession>) {
//     if let Some(sess) = session {
//         let resp = sess.conn.keep_alive(rpc::KeepAliveRequest{session_id: sess.session_id.clone()}).await;
//         if resp.is_err() {
//             return;
//         }
//     }
// }

// async fn send_keep_alive(session: &mut ChubbyClientSession,
// tx: tokio::sync::oneshot::Sender<tonic::Response<chubby_server::rpc::KeepAliveResponse>>) {
//     let result = session.conn.keep_alive(rpc::KeepAliveRequest{session_id: session.session_id.clone()}).await;
//     if let Ok(resp) = result {
//         tx.send(resp);
//     }
// }

async fn create_timeout(duration: Duration) {
    tokio::spawn(tokio::time::sleep(duration)).await.unwrap();
}

async fn send_keep_alive(
    session_id: String,
    tx: tokio::sync::oneshot::Sender<tonic::Response<chubby_server::rpc::KeepAliveResponse>>,
) {
    let mut conn = rpc::chubby_client::ChubbyClient::connect("http://127.0.0.1:50051").await;
    if let Ok(conn) = &mut conn {
        let result = conn.keep_alive(rpc::KeepAliveRequest { session_id }).await;
        if let Ok(resp) = result {
            tx.send(resp);
        }
    }
}

impl ChubbyClient {
    pub async fn new() -> Result<ChubbyClient, tonic::transport::Error> {
        println!("ChubbyClient::new()");
        return Ok(Self {
            session: Arc::new(tokio::sync::Mutex::new(None)),
        });
    }

    pub async fn create_session(&mut self) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        println!("ChubbyClient::create_session()");
        // TODO: Check if connection is valid, else re-establish
        if let Some(session) = &*self.session.lock().await {
            return Err(Box::new(ChubbyClientError::SessionInProgress(
                session.session_id.clone(),
            )));
        }

        // TODO: Keep retrying till connection succeeds
        let mut conn = rpc::chubby_client::ChubbyClient::connect("http://127.0.0.1:50051").await?;
        let resp = conn.create_session(rpc::CreateSessionRequest {}).await?;
        let session_id = &resp.get_ref().session_id;
        let lease_length = resp.get_ref().lease_length;
        println!("\treceived session id: {}", session_id);
        let session = ChubbyClientSession {
            session_id: session_id.clone(),
            lease_length: Duration::from_secs(lease_length),
            conn,
        };
        *self.session.lock().await = Some(session);
        let shared_session = self.session.clone();
        let session_id_clone = session_id.clone();
        tokio::task::spawn(async move {
            println!("\tspawning task for session ID {}", session_id_clone);
            loop {
                let mut sess = shared_session.lock().await;
                let session_option = &mut (*sess);
                if let Some(session) = session_option {
                    let (tx, rx) = oneshot::channel::<
                        tonic::Response<chubby_server::rpc::KeepAliveResponse>,
                    >();
                    // Read all data from session needed to make the RPC and
                    // release the lock before making the RPC
                    let session_id = session.session_id.clone();
                    let lease_length = session.lease_length;
                    drop(sess);
                    let timeout = create_timeout(lease_length);
                    send_keep_alive(session_id, tx).await;
                    tokio::select! {
                        _ = timeout => {
                            println!("session lease expired, jeopardy begins");
                            return;
                        }
                        v = rx => {
                            println!("got = {:?}", v);
                        }

                    }
                } else {
                    // println!("\tfinishing task for session ID {}", session_id_clone);
                    return;
                }
            }
        });
        // thread::sleep(time::Duration::from_millis(100));
        return Ok(());
    }

    pub async fn delete_session(&mut self) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        println!("ChubbyClient::delete_session()");
        // TODO: Check if connection is valid, else re-establish
        println!("\tacquiring lock");
        let session_option = &mut (*self.session.lock().await);
        println!("\tacquired lock");
        if let Some(session) = session_option {
            let resp = session
                .conn
                .delete_session(rpc::DeleteSessionRequest {
                    session_id: session.session_id.clone(),
                })
                .await?;
            let success = resp.into_inner().success;
            if success {
                println!("\tsetting session ID {} to None", session.session_id);
                *session_option = None;
            }
            // thread::sleep(time::Duration::from_millis(100));
        } else {
            println!("\tsession doesn't exist");
            return Err(Box::new(ChubbyClientError::SessionDoesNotExist));
        }
        return Ok(());
    }
}
