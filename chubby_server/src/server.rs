mod rpc;
use atomic_counter::AtomicCounter;
use atomic_counter::ConsistentCounter;
use rpc::chubby_server::Chubby;
use tonic::{transport::Server, Request, Response, Status};

pub struct ChubbyServer {
    pub addr: std::net::SocketAddr,
    pub session_counter: ConsistentCounter,
}

#[tonic::async_trait]
impl Chubby for ChubbyServer {
    async fn create_session(
        &self,
        request: tonic::Request<rpc::CreateSessionRequest>,
    ) -> Result<tonic::Response<rpc::CreateSessionResponse>, tonic::Status> {
        println!("Received create_session request");
        self.session_counter.inc();
        println!("\tcounter = {}", self.session_counter.get());
        Ok(Response::new(rpc::CreateSessionResponse {
            session_id: self.session_counter.get().to_string(),
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
    };
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(rpc::chubby_server::ChubbyServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}
