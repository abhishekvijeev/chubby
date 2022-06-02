//! This module contains implementation and functions for returning [std::error::Error] and [Result] type
//! objects from Tribbler related functions.
use std::{error::Error, fmt::Display};

// basic error types that can occur when running the tribbler service.
#[derive(Debug, Clone)]
pub enum ChubbyClientError {
    /// when a lock wasn't able to be acquired
    LockNotAcquired(String),
    /// when a lock wasn't able to be released
    LockNotReleased(String),
    /// when there are no more seq numbers to give out
    MaxedSeq,
    /// generic error for anything that occurs with RPC communication
    RpcError(String),
    /// client has already established a session with the Chubby server
    SessionInProgress(String),
    /// session has not been established with the Chubby Server
    SessionDoesNotExist,
    /// catch-all error for other issues
    Unknown(String),
}

impl Display for ChubbyClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let x = match self {
            ChubbyClientError::LockNotAcquired(x) => {
                format!("server was not able to acquire lock {}", x)
            }
            ChubbyClientError::LockNotReleased(x) => {
                format!("server was not able to release lock {}", x)
            }
            ChubbyClientError::MaxedSeq => "server has run out of sequence numbers".to_string(),
            ChubbyClientError::RpcError(x) => format!("rpc error: {}", x),
            ChubbyClientError::SessionInProgress(x) => {
                format!("session with id {} is in progress", x)
            }
            ChubbyClientError::SessionDoesNotExist => {
                "there is no currently existing session with the chubby server".to_string()
            }
            x => format!("{:?}", x),
        };
        write!(f, "{}", x)
    }
}

impl std::error::Error for ChubbyClientError {}

impl From<tonic::Status> for ChubbyClientError {
    fn from(v: tonic::Status) -> Self {
        ChubbyClientError::RpcError(format!("{:?}", v))
    }
}

impl From<tonic::transport::Error> for ChubbyClientError {
    fn from(v: tonic::transport::Error) -> Self {
        ChubbyClientError::RpcError(format!("{:?}", v))
    }
}

/// A [Result] type which either returns `T` or a [boxed error](https://doc.rust-lang.org/rust-by-example/error/multiple_error_types/boxing_errors.html)
pub type ChubbyClientResult<T> = Result<T, Box<(dyn Error + Send + Sync)>>;

impl From<Box<dyn Error>> for ChubbyClientError {
    fn from(x: Box<dyn Error>) -> Self {
        ChubbyClientError::Unknown(x.to_string())
    }
}
