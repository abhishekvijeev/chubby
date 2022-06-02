mod constants;
mod rpc;
use atomic_counter::AtomicCounter;
use atomic_counter::ConsistentCounter;
use chubby_server::raft::Raft;
use rpc::acquire_request::Mode;
use rpc::chubby_server::Chubby;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio;
use tokio::sync::broadcast;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Deserialize, Serialize)]
struct Lock {
    path: String,
    mode: constants::LockMode,
    content: String,
    acquired_by: HashSet<usize>,
    fence_token: u64,
    ref_cnt: u64, // used only for shared locks
}

struct ChubbySession {
    session_id: usize,
    start_time: Instant,
    lease_length: Duration,
    session_renew_sender: broadcast::Sender<u32>,
    session_timeout_sender: broadcast::Sender<u32>,
    expired: bool,
}

pub struct ChubbyServer {
    addr: std::net::SocketAddr,
    session_counter: ConsistentCounter,
    sessions: Arc<tokio::sync::Mutex<HashMap<usize, ChubbySession>>>,
    kvstore: Arc<tokio::sync::Mutex<Raft>>,
    locks: Arc<tokio::sync::Mutex<HashMap<String, Lock>>>,
    fence_token_counter: ConsistentCounter,
}

#[tonic::async_trait]
impl Chubby for ChubbyServer {
    async fn create_session(
        &self,
        request: tonic::Request<rpc::CreateSessionRequest>,
    ) -> Result<tonic::Response<rpc::CreateSessionResponse>, tonic::Status> {
        println!("Received create_session request");
        let session_id = self.session_counter.inc();
        let session_id_clone = session_id.clone();
        let (session_renew_sender, _) = broadcast::channel::<u32>(100);
        let (session_timeout_sender, _) = broadcast::channel::<u32>(100);
        let session = ChubbySession {
            session_id,
            start_time: Instant::now(),
            lease_length: constants::LEASE_EXTENSION,
            session_renew_sender,
            session_timeout_sender,
            expired: false,
        };
        self.sessions.lock().await.insert(session_id, session);
        let shared_session = self.sessions.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let mut sessions_map = shared_session.lock().await;
                let session_option = sessions_map.get_mut(&session_id);
                if let Some(session) = session_option {
                    // println!(
                    //     "Session {} time elapsed: {:?}, lease length: {:?}",
                    //     session_id,
                    //     session.start_time.elapsed(),
                    //     session.lease_length
                    // );
                    if session.start_time.elapsed() > session.lease_length {
                        println!("Session ID {}'s lease has expired", session_id_clone);
                        session.expired = true;
                        let r = session.session_timeout_sender.send(1);
                        if r.is_err() {
                            println!("Could not send message to timeout channel");
                        }
                        return;
                    } else if (session.start_time + session.lease_length)
                        - std::time::Instant::now()
                        <= Duration::from_secs(1)
                    {
                        // check if we're close to the lease expiring - if so, renew the lease and
                        // send a message to KeepAlive so that it can return (if there is a
                        // pending keepalive that's blocked)
                        session.session_renew_sender.send(1);
                    }
                } else {
                    return;
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
        let session_id = request.into_inner().session_id.parse::<usize>().unwrap();
        let mut sessions_map = self.sessions.lock().await;
        let session_option = sessions_map.get_mut(&session_id);
        if let Some(session) = session_option {
            session.expired = true;
        }

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

        if session.expired {
            return Ok(Response::new(rpc::KeepAliveResponse {
                expired: true,
                lease_length: constants::LEASE_EXTENSION.as_secs(),
            }));
        }

        let mut timeout_receiver = session.session_timeout_sender.subscribe();
        let mut renew_receiver = session.session_renew_sender.subscribe();
        let lease_length = session.lease_length;
        drop(sessions_map);
        tokio::select! {
            v = timeout_receiver.recv() => {
                println!("session {} has timed-out", session_id);
                // TODO: self.release_locks(session_id);
                return Ok(Response::new(rpc::KeepAliveResponse { expired: true, lease_length: constants::LEASE_EXTENSION.as_secs() }));
            }
            v = renew_receiver.recv() => {
                println!("session {}'s lease is being renewed by {}s", session_id, constants::LEASE_EXTENSION.as_secs());
                let mut sessions_map = self.sessions.lock().await;
                let session_option = sessions_map.get_mut(&session_id);
                if let Some(session) = session_option {
                    session.lease_length += constants::LEASE_EXTENSION;
                    return Ok(Response::new(rpc::KeepAliveResponse { expired: false, lease_length: constants::LEASE_EXTENSION.as_secs() }));
                } else {
                    return Ok(Response::new(rpc::KeepAliveResponse { expired: true, lease_length: constants::LEASE_EXTENSION.as_secs() }));
                }
            }
        }
    }

    async fn open(
        &self,
        request: tonic::Request<rpc::OpenRequest>,
    ) -> Result<tonic::Response<rpc::OpenResponse>, tonic::Status> {
        let request = request.into_inner();
        let session_id = request.session_id.parse::<usize>().unwrap();
        let sessions_map = self.sessions.lock().await;
        let session = &sessions_map[&session_id];

        if session.expired {
            return Ok(Response::new(rpc::OpenResponse { expired: true }));
        }

        // TODO: Validate path and return error for invalid paths
        let path = request.path;

        // Checks that the lock path doesn't already exist
        let resp = self.kvstore.lock().await.get(path.clone());
        if resp.is_none() {
            let lock = Lock {
                path: path.clone(),
                mode: constants::LockMode::FREE,
                content: "".to_string(),
                acquired_by: HashSet::new(),
                fence_token: 0,
                ref_cnt: 0,
            };
            let serialized_lock = serde_json::to_string(&lock).unwrap();
            self.kvstore
                .lock()
                .await
                .propose_normal(path.clone(), serialized_lock);
            self.locks.lock().await.insert(path, lock);
        }
        Ok(Response::new(rpc::OpenResponse { expired: false }))
    }

    async fn acquire(
        &self,
        request: tonic::Request<rpc::AcquireRequest>,
    ) -> Result<tonic::Response<rpc::AcquireResponse>, tonic::Status> {
        let request = request.into_inner();
        let session_id = request.session_id.parse::<usize>().unwrap();
        let sessions_map = self.sessions.lock().await;
        let session = &sessions_map[&session_id];
        let acquire_request_mode = request.mode;

        if session.expired {
            return Ok(Response::new(rpc::AcquireResponse {
                expired: true,
                acquired_lock: false,
                fence_token: 1,
            }));
        }

        let path = request.path;
        // TODO: Validate path and return error for invalid paths

        let resp = self.kvstore.lock().await.get(path.clone());
        if resp.is_none() {
            return Err(tonic::Status::not_found(
                "Lock not found - please open the lock file before trying to acquire the lock",
            ));
        }

        let mut locks_map = self.locks.lock().await;
        if !locks_map.contains_key(&path) {
            // The KV store has the lock and the server doesn't.
            // This is because the server recovered from a failure
            // due to which it lost its volatile data structures.
            // We therefore reconstruct the lock in its map.
            let lock_str = resp.unwrap();
            let deserialized_lock: Lock = serde_json::from_str(&lock_str).unwrap();
            locks_map.insert(path.clone(), deserialized_lock);
        }
        let lock = locks_map.get_mut(&path).unwrap();
        let curr_lock_mode = lock.mode.clone();

        match Mode::from_i32(acquire_request_mode) {
            Some(Mode::Exclusive) => {
                if matches!(curr_lock_mode, constants::LockMode::EXCLUSIVE) {
                    return Err(tonic::Status::unavailable(
                        "Lock is currently held in EXCLUSIVE mode and therefore unavailable for acquisition in EXCLUSIVE mode",
                    ));
                } else if matches!(curr_lock_mode, constants::LockMode::SHARED) {
                    return Err(tonic::Status::unavailable(
                        "Lock is currently held in SHARED mode and therefore unavailable for acquisition in EXCLUSIVE mode",
                    ));
                }
                let mut locks_map = self.locks.lock().await;
                let lock = locks_map.get_mut(&path).unwrap();
                let fence_token = self.fence_token_counter.inc() as u64;
                lock.mode = constants::LockMode::EXCLUSIVE;
                lock.fence_token = fence_token;
                lock.acquired_by.insert(session_id);
                let serialized_lock = serde_json::to_string(lock).unwrap();
                drop(locks_map);
                self.kvstore
                    .lock()
                    .await
                    .propose_normal(path.clone(), serialized_lock);
                return Ok(Response::new(rpc::AcquireResponse {
                    expired: false,
                    acquired_lock: true,
                    fence_token: fence_token as u64,
                }));
            }
            Some(Mode::Shared) => {
                if matches!(curr_lock_mode, constants::LockMode::EXCLUSIVE) {
                    return Err(tonic::Status::unavailable(
                        "Lock is currently held in EXCLUSIVE mode and therefore unavailable for acquisition in SHARED mode",
                    ));
                } else if matches!(curr_lock_mode, constants::LockMode::SHARED) {
                    let mut locks_map = self.locks.lock().await;
                    let lock = locks_map.get_mut(&path).unwrap();
                    let fence_token = lock.fence_token;
                    lock.acquired_by.insert(session_id);
                    lock.ref_cnt += 1;
                    let serialized_lock = serde_json::to_string(lock).unwrap();
                    drop(locks_map);
                    self.kvstore
                        .lock()
                        .await
                        .propose_normal(path.clone(), serialized_lock);
                    return Ok(Response::new(rpc::AcquireResponse {
                        expired: false,
                        acquired_lock: true,
                        fence_token: fence_token as u64,
                    }));
                } else {
                    let mut locks_map = self.locks.lock().await;
                    let lock = locks_map.get_mut(&path).unwrap();
                    let fence_token = self.fence_token_counter.inc() as u64;
                    lock.mode = constants::LockMode::SHARED;
                    lock.fence_token = fence_token;
                    lock.acquired_by.insert(session_id);
                    lock.ref_cnt += 1;
                    let serialized_lock = serde_json::to_string(lock).unwrap();
                    drop(locks_map);
                    self.kvstore
                        .lock()
                        .await
                        .propose_normal(path.clone(), serialized_lock);
                    return Ok(Response::new(rpc::AcquireResponse {
                        expired: false,
                        acquired_lock: true,
                        fence_token: fence_token as u64,
                    }));
                }
            }
            None => {
                return Err(tonic::Status::invalid_argument(
                    "Lock acquisition mode must be one of EXCLUSIVE or SHARED",
                ));
            }
        }
    }

    async fn release(
        &self,
        request: tonic::Request<rpc::ReleaseRequest>,
    ) -> Result<tonic::Response<rpc::ReleaseResponse>, tonic::Status> {
        let request = request.into_inner();
        let session_id = request.session_id.parse::<usize>().unwrap();
        let sessions_map = self.sessions.lock().await;
        let session = &sessions_map[&session_id];

        if session.expired {
            return Ok(Response::new(rpc::ReleaseResponse {
                expired: true,
                released_lock: false,
            }));
        }

        let path = request.path;
        // TODO: Validate path and return error for invalid paths

        let resp = self.kvstore.lock().await.get(path.clone());
        if resp.is_none() {
            return Err(tonic::Status::not_found(
                "Lock not found - please open and acquire the lock file before trying to release the lock",
            ));
        }

        let mut locks_map = self.locks.lock().await;
        if !locks_map.contains_key(&path) {
            // The KV store has the lock and the server doesn't.
            // This is because the server recovered from a failure
            // due to which it lost its volatile data structures.
            // We therefore reconstruct the lock in its map.
            let lock_str = resp.unwrap();
            let deserialized_lock: Lock = serde_json::from_str(&lock_str).unwrap();
            locks_map.insert(path.clone(), deserialized_lock);
        }
        let lock = locks_map.get_mut(&path).unwrap();
        let curr_lock_mode = lock.mode.clone();
        if !lock.acquired_by.contains(&session_id) {
            return Err(tonic::Status::failed_precondition(
                "Lock must be acquired before attempting to release it",
            ));
        }

        // Check fence token
        if lock.fence_token != request.fence_token {
            return Err(tonic::Status::invalid_argument(
                "Lock's fence token doesn't match the request's fence token - please reaquire the lock",
            ));
        }

        match curr_lock_mode {
            constants::LockMode::EXCLUSIVE => {
                lock.mode = constants::LockMode::FREE;
                let serialized_lock = serde_json::to_string(lock).unwrap();
                drop(locks_map);
                self.kvstore
                    .lock()
                    .await
                    .propose_normal(path.clone(), serialized_lock);
            }
            constants::LockMode::SHARED => {
                lock.ref_cnt -= 1;
                lock.acquired_by.remove(&session_id);
                if lock.ref_cnt == 0 {
                    lock.mode = constants::LockMode::FREE;
                }
                let serialized_lock = serde_json::to_string(lock).unwrap();
                drop(locks_map);
                self.kvstore
                    .lock()
                    .await
                    .propose_normal(path.clone(), serialized_lock);
            }
            constants::LockMode::FREE => {
                return Err(tonic::Status::failed_precondition(
                    "Lock must be acquired before attempting to release it",
                ));
            }
        }

        Ok(Response::new(rpc::ReleaseResponse {
            expired: false,
            released_lock: true,
        }))
    }

    async fn get_contents(
        &self,
        request: tonic::Request<rpc::GetContentsRequest>,
    ) -> Result<tonic::Response<rpc::GetContentsResponse>, tonic::Status> {
        let request = request.into_inner();
        let session_id = request.session_id.parse::<usize>().unwrap();
        let sessions_map = self.sessions.lock().await;
        let session = &sessions_map[&session_id];

        if session.expired {
            return Ok(Response::new(rpc::GetContentsResponse {
                expired: true,
                get_contents_status: false,
                contents: String::from(""),
            }));
        }

        let path = request.path;
        // TODO: Validate path and return error for invalid paths

        Ok(Response::new(rpc::GetContentsResponse {
            expired: false,
            get_contents_status: true,
            contents: String::from(""),
        }))
    }

    async fn set_contents(
        &self,
        request: tonic::Request<rpc::SetContentsRequest>,
    ) -> Result<tonic::Response<rpc::SetContentsResponse>, tonic::Status> {
        let request = request.into_inner();
        let session_id = request.session_id.parse::<usize>().unwrap();
        let sessions_map = self.sessions.lock().await;
        let session = &sessions_map[&session_id];

        if session.expired {
            return Ok(Response::new(rpc::SetContentsResponse {
                expired: true,
                set_contents_status: false,
            }));
        }

        let path = request.path;
        // TODO: Validate path and return error for invalid paths

        Ok(Response::new(rpc::SetContentsResponse {
            expired: false,
            set_contents_status: true,
        }))
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
        kvstore: Arc::new(tokio::sync::Mutex::new(raft)),
        locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        fence_token_counter: ConsistentCounter::new(0),
    };
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(rpc::chubby_server::ChubbyServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}
