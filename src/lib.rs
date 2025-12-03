use std::io;

use thiserror::Error;

use crate::packet::{MessageType, RequestType, Response, ResponseType};

pub mod client;
pub mod server;
pub mod packet;
pub mod store;

const CAPACITY: usize = 8500000;
const BE_BYTE: u8 = 6;
const LC_READ_BYTE: u8 = 7;
const LC_WRITE_BYTE: u8 = 8;
const NONE_BYTE: u8 = 0;
const SOME_BYTE: u8 = 1;
const SUBSTRING_LEN: usize = 3;
const BUF_LEN: usize = 512;
const LEN_LENGTH: usize = size_of::<u64>();
const SIG_FIG: u8 = 3;
const YIELD_FREQ: usize = 5; // yield every 2^n best effort sub-operations

#[derive(Debug, Error)]
pub enum AspenRsError {
  #[error("network error: {0}")]
  NetworkError(#[from] NetworkError),
  #[error("parse error: {0}")]
  ParseError(#[from] ParseError)
}

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("connection closed cleanly by peer")]
    ConnectionClosed,

    #[error("connection reset by peer")]
    ConnectionReset,

    #[error("operation interrupted")]
    Interrupted,

    #[error("unexpected io error: {0}")]
    Io(std::io::Error),
}

impl From<std::io::Error> for NetworkError {
    fn from(value: std::io::Error) -> Self {
        match value.kind() {
          io::ErrorKind::ConnectionReset => NetworkError::ConnectionReset,
          io::ErrorKind::Interrupted => NetworkError::Interrupted,
          _ => NetworkError::Io(value),
        }
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
  #[error("payload len {payload_len} not equal to expected len {exp_len}")]
  UnexpectedLength{payload_len: usize, exp_len: usize},
  #[error("malformed packet: {0}")]
  MalformedPacket(String),
  #[error("value {0} is not attributed to a request type")]
  InvalidMessageType(u8),
  #[error("value {0} is not attributed to a option type")]
  UnexpectedOptionType(u8),
  #[error("packet too short")]
  PacketTooShort,
  #[error("expected message of type {:?} but parsed message with type {:?}", given_type, exp_type)]
  UnexpectedMessageType{given_type: ResponseType, exp_type: ResponseType},
}