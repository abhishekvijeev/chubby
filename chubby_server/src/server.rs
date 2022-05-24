mod constants;
mod rpc;
use atomic_counter::AtomicCounter;
use atomic_counter::ConsistentCounter;
use chubby_server::raft::Raft;
use rpc::chubby_server::Chubby;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio;
use tokio::sync::broadcast;
use tonic::{transport::Server, Request, Response, Status};

pub struct ChubbySession {
    session_id: usize,
    start_time: Instant,
    lease_length: Duration,
    session_renew_sender: broadcast::Sender<u32>,
    // session_renew_receiver: broadcast::Receiver<u32>,
    session_timeout_sender: broadcast::Sender<u32>,
    // session_timeout_receiver: broadcast::Receiver<u32>,
}

pub struct ChubbyServer {
    pub addr: std::net::SocketAddr,
    pub session_counter: ConsistentCounter,
    pub sessions: Arc<tokio::sync::Mutex<HashMap<usize, ChubbySession>>>,
}

#[tonic::async_trait]
impl Chubby for ChubbyServer {
    async fn create_session(
        &self,
        request: tonic::Request<rpc::CreateSessionRequest>,
    ) -> Result<tonic::Response<rpc::CreateSessionResponse>, tonic::Status> {
        println!("Received create_session request");
        self.session_counter.inc();
        let session_id = self.session_counter.get();
        let session_id_clone = session_id.clone();
        let (session_renew_sender, _) = broadcast::channel::<u32>(100);
        let (session_timeout_sender, _) = broadcast::channel::<u32>(100);
        let session = ChubbySession {
            session_id,
            start_time: Instant::now(),
            lease_length: constants::LEASE_EXTENSION,
            session_renew_sender,
            // session_renew_receiver,
            session_timeout_sender,
            // session_timeout_receiver,
        };
        self.sessions.lock().await.insert(session_id, session);
        let shared_session = self.sessions.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let sessions_map = shared_session.lock().await;
                let session = &sessions_map[&session_id_clone];
                if session.start_time.elapsed() > session.lease_length {
                    println!("Session ID {}'s lease has expired", session_id_clone);
                    session.session_timeout_sender.send(1);
                    return;
                } else if (session.start_time + session.lease_length) - std::time::Instant::now()
                    <= Duration::from_secs(2)
                {
                    // check if we're close to the lease expiring - if so, renew the lease and
                    // send a message to KeepAlive so that it can return (if there is a
                    // pending keepalive that's blocked)
                    println!("Session ID {}'s lease is being renewed", session_id_clone);
                    session.session_renew_sender.send(1);
                }
            }
        });

        Ok(Response::new(rpc::CreateSessionResponse {
            session_id: session_id.to_string(),
            lease_length: constants::LEASE_EXTENSION.as_secs(),
        }))
    }

    async fn delete_session(
        &self,
        request: tonic::Request<rpc::DeleteSessionRequest>,
    ) -> Result<tonic::Response<rpc::DeleteSessionResponse>, tonic::Status> {
        println!("Received delete_session request");
        Ok(Response::new(rpc::DeleteSessionResponse { success: true }))
    }

    async fn keep_alive(
        &self,
        request: tonic::Request<rpc::KeepAliveRequest>,
    ) -> Result<tonic::Response<rpc::KeepAliveResponse>, tonic::Status> {
        println!("Received keep_alive request");
        let session_id = request.into_inner().session_id.parse::<usize>().unwrap();
        let sessions_map = self.sessions.lock().await;
        let session = &sessions_map[&session_id];
        let mut timeout_receiver = session.session_timeout_sender.subscribe();
        let mut renew_receiver = session.session_renew_sender.subscribe();
        drop(sessions_map);
        tokio::select! {
            v = timeout_receiver.recv() => {
                println!("session {} has timed-out", session_id);
            }
            v = renew_receiver.recv() => {
                println!("session {}'s lease is being renewed", session_id);
            }
        }
        Ok(Response::new(rpc::KeepAliveResponse { lease_length: 1 }))
    }

    async fn open(
        &self,
        request: tonic::Request<rpc::OpenRequest>,
    ) -> Result<tonic::Response<rpc::OpenResponse>, tonic::Status> {
        Ok(Response::new(rpc::OpenResponse {}))
    }

    async fn acquire(
        &self,
        request: tonic::Request<rpc::AcquireRequest>,
    ) -> Result<tonic::Response<rpc::AcquireResponse>, tonic::Status> {
        Ok(Response::new(rpc::AcquireResponse {
            status: true,
            fence_token: 1,
        }))
    }

    async fn release(
        &self,
        request: tonic::Request<rpc::ReleaseRequest>,
    ) -> Result<tonic::Response<rpc::ReleaseResponse>, tonic::Status> {
        Ok(Response::new(rpc::ReleaseResponse { status: true }))
    }

    async fn get_contents(
        &self,
        request: tonic::Request<rpc::GetContentsRequest>,
    ) -> Result<tonic::Response<rpc::GetContentsResponse>, tonic::Status> {
        Ok(Response::new(rpc::GetContentsResponse {
            status: true,
            contents: String::from(""),
        }))
    }

    async fn set_contents(
        &self,
        request: tonic::Request<rpc::SetContentsRequest>,
    ) -> Result<tonic::Response<rpc::SetContentsResponse>, tonic::Status> {
        Ok(Response::new(rpc::SetContentsResponse { status: true }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut raft = Raft::new(constants::NUM_NODES);
    raft.init_raft();

    let addr = "127.0.0.1:50051".parse().unwrap();
    let server = ChubbyServer {
        addr,
        session_counter: ConsistentCounter::new(0),
        sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(rpc::chubby_server::ChubbyServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}
