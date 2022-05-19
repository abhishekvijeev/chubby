mod constants;
mod rpc;
use atomic_counter::AtomicCounter;
use atomic_counter::ConsistentCounter;
use rpc::chubby_server::Chubby;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tonic::{transport::Server, Request, Response, Status};

pub struct ChubbySession {
    session_id: usize,
    start_time: Instant,
    lease_length: Duration,
}

pub struct ChubbyServer {
    pub addr: std::net::SocketAddr,
    pub session_counter: ConsistentCounter,
    pub sessions: Arc<RwLock<HashMap<usize, ChubbySession>>>,
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

        let session = ChubbySession {
            session_id: session_id,
            start_time: Instant::now(),
            lease_length: constants::LEASE_EXTENSION,
        };

        self.sessions.write().unwrap().insert(session_id, session);
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();
    let server = ChubbyServer {
        addr: addr,
        session_counter: ConsistentCounter::new(0),
        sessions: Arc::new(RwLock::new(HashMap::new())),
    };
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(rpc::chubby_server::ChubbyServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}
