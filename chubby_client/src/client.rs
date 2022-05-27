use crate::err::ChubbyClientError;
use crate::constants;
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
    in_jeopardy: bool,
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
    keep_alive_sender: tokio::sync::oneshot::Sender<tonic::Response<chubby_server::rpc::KeepAliveResponse>>,
) {
    let mut conn = rpc::chubby_client::ChubbyClient::connect("http://127.0.0.1:50051").await;
    if let Ok(conn) = &mut conn {
        let result = conn.keep_alive(rpc::KeepAliveRequest { session_id }).await;
        if let Ok(resp) = result {
            keep_alive_sender.send(resp);
        }
    }
}

async fn start_jeopardy(session_arc: Arc<tokio::sync::Mutex<Option<ChubbyClientSession>>>) {
    let mut sess = session_arc.lock().await;
    let session_option = &mut (*sess);
    if let Some(session) = session_option {
        session.in_jeopardy = true;
        let session_id = session.session_id.clone();
        let (keep_alive_sender, keep_alive_receiver) = oneshot::channel::<
            tonic::Response<chubby_server::rpc::KeepAliveResponse>,
        >();
        drop(sess);
        tokio::task::spawn(async move {
            try_end_jeopardy(session_id, keep_alive_sender).await;
        });

        let timeout = create_timeout(constants::JEOPARDY_DURATION);
        tokio::select! {
            _ = timeout => {
                println!("jeopardy period has expired - terminating session");
                let mut sess = session_arc.lock().await;
                let session_option = &mut (*sess);
                *session_option = None;
                // if let Some(session) = session_option {
                //     session.expired = true;
                // }
            }
            keep_alive_result = keep_alive_receiver => {
                println!("session has been revived!");
                let mut sess = session_arc.lock().await;
                let session_option = &mut (*sess);
                if let Some(session) = session_option {
                    if let Ok(response) = keep_alive_result {
                        // TODO: Fix different lease length semantics
                        // at client and server
                        let lease_length = response.into_inner().lease_length;
                        session.lease_length = Duration::from_secs(lease_length);
                        session.in_jeopardy = false;
                    }
                    
                }
            }
        }
    }
}

async fn try_end_jeopardy(session_id: String,
    keep_alive_sender: tokio::sync::oneshot::Sender<tonic::Response<chubby_server::rpc::KeepAliveResponse>>) {
    let server_list = vec!["http://127.0.0.1:50051"];
    loop {
        for &server_addr in server_list.iter() {
            // try to contact server 'i'
            // if successful, jeopardy is over, session has been saved
            let mut conn_status = rpc::chubby_client::ChubbyClient::connect(server_addr).await;
            if let Ok(conn) = &mut conn_status {
                println!("successfully established connection with server {}, sending KeepAlive", server_addr);
                let result = conn.keep_alive(rpc::KeepAliveRequest { session_id: session_id.clone() }).await;
                if let Ok(resp) = result {
                    println!("received KeepAlive response from server{}", server_addr);
                    keep_alive_sender.send(resp);
                    return;
                }
            }
        }
    }
}

async fn monitor_session(session_arc: Arc<tokio::sync::Mutex<Option<ChubbyClientSession>>>) {
    loop {
        let mut sess = session_arc.lock().await;
        let session_option = &mut (*sess);
        if let Some(session) = session_option {
            let (keep_alive_sender, keep_alive_receiver) = oneshot::channel::<
                tonic::Response<chubby_server::rpc::KeepAliveResponse>,
            >();
            // Read all data from session needed to make the RPC and
            // release the lock before making the RPC
            let session_id = session.session_id.clone();
            let lease_length = session.lease_length;
            drop(sess);

            // TODO: Check if the timeout is correct
            // TODO: Fix different lease length semantics
            // at client and server
            let timeout = create_timeout(lease_length);
            send_keep_alive(session_id.clone(), keep_alive_sender).await;
            tokio::select! {
                _ = timeout => {
                    println!("session lease expired, jeopardy begins");
                    // After start_jeopardy() returns, one of two things happens:
                    // 1) The session is saved and then next iteration of this loop would
                    // proceed as normal
                    // 2) The session is terminated and set to None, in which case the
                    // loop stops execution
                    start_jeopardy(session_arc.clone()).await;
                }
                keep_alive_result = keep_alive_receiver => {
                    let mut sess = session_arc.lock().await;
                    let session_option = &mut (*sess);
                    if let Some(session) = session_option {
                    
                        if let Ok(response) = keep_alive_result {
                            // TODO: Fix different lease length semantics
                            // at client and server
                            let lease_length = response.into_inner().lease_length;
                            println!("resetting lease of session {} to {:?}", session_id, lease_length);
                            session.lease_length = Duration::from_secs(lease_length);
                        }
                    }
                }
            }
        } else {
            // println!("\tfinishing task for session ID {}", session_id_clone);
            return;
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
            in_jeopardy: false,
        };
        *self.session.lock().await = Some(session);
        let shared_session = self.session.clone();
        let session_id_clone = session_id.clone();
        tokio::task::spawn(async move {
            println!("\tspawning task to monitor session ID {}", session_id_clone);
            monitor_session(shared_session).await;
        });
        // thread::sleep(time::Duration::from_millis(100));
        return Ok(());
    }

    pub async fn delete_session(&mut self) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        println!("ChubbyClient::delete_session()");
        // TODO: Check if connection is valid, else re-establish
        // TODO: Release lock before calling await
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

    pub async fn open(&mut self, path: String) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        println!("ChubbyClient::open()");
        // TODO: Check if connection is valid, else re-establish
        // TODO: Release lock before calling await
        println!("\tacquiring lock");
        let session_option = &mut (*self.session.lock().await);
        println!("\tacquired lock");
        if let Some(session) = session_option {
            let resp = session
                .conn
                .open(rpc::OpenRequest { path: path.clone() })
                .await?;
        } else {
            println!("\tsession doesn't exist");
            return Err(Box::new(ChubbyClientError::SessionDoesNotExist));
        }
        return Ok(());
    }

    pub async fn acquire(&mut self, path: String) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        return Ok(());
    }

    pub async fn release(&mut self, path: String) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        return Ok(());
    }

    pub async fn get_contents(
        &mut self,
        path: String,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        return Ok(());
    }

    pub async fn set_contents(
        &mut self,
        path: String,
        contents: String,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        return Ok(());
    }
}
