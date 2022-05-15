use crate::err::ChubbyClientError;
use chubby_server::rpc;
use std::error::Error;
use tonic::transport::channel::Channel;

pub struct ChubbyClient {
    session: Option<ChubbyClientSession>,
    conn: rpc::chubby_client::ChubbyClient<Channel>,
}

struct ChubbyClientSession {
    session_id: String,

}

impl ChubbyClient {
    pub async fn new() -> Result<ChubbyClient, tonic::transport::Error> {
        // TODO: Keep retrying till connection succeeds
        let conn = rpc::chubby_client::ChubbyClient::connect("http://127.0.0.1:50051").await?;
        return Ok(Self {
            session: None,
            conn: conn,
        });
    }

    pub async fn create_session(&mut self) -> Result<(), Box<(dyn Error + Send + Sync)>>{
        // TODO: Check if connection is valid, else re-establish
        if let Some(session) = &self.session {
            return Err(Box::new(ChubbyClientError::SessionInProgress(session.session_id.clone())));
        }
        
        let resp = self.conn.create_session(rpc::CreateSessionRequest{}).await?;
        let session_id = resp.into_inner().session_id;
        self.session = Some(ChubbyClientSession {
            session_id: session_id,
        });
        return Ok(());
    }

    pub async fn delete_session(&mut self) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        // TODO: Check if connection is valid, else re-establish
        if let Some(session) = &self.session {
            let resp = self.conn.delete_session(rpc::DeleteSessionRequest{session_id: session.session_id.clone()}).await?;
            let success = resp.into_inner().success;
            if success {
                self.session = None;
            }
        }
        else {
            return Err(Box::new(ChubbyClientError::SessionDoesNotExist));
        }
        return Ok(());
    }
}