use rand::{Rng, distr::{Alphanumeric, SampleString}};
use crate::{BE_BYTE, CAPACITY, LC_READ_BYTE, LC_WRITE_BYTE, LEN_LENGTH, NONE_BYTE, ParseError, SOME_BYTE, SUBSTRING_LEN};

pub trait Message {
  type Tag: MessageType;
  fn kind(&self) -> Self::Tag;
  fn serialize(&self) -> Vec<u8>;
  fn deserialize(packet: &[u8]) -> Result<Self, ParseError> where Self: std::marker::Sized;
}

pub trait MessageType {
  fn value(&self) -> u8;
  fn from_value(value: u8) -> Result<Self, ParseError> where Self: std::marker::Sized;
  fn expected_len(&self) -> Option<usize>;
  fn iterator() -> impl Iterator<Item = Self>;
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, Debug)]
pub enum RequestType {
  BeRead,
  LcRead,
  LcWrite
}

impl MessageType for RequestType {
    fn value(&self) -> u8 {
        match &self {
            RequestType::BeRead => BE_BYTE,
            RequestType::LcRead => LC_READ_BYTE,
            RequestType::LcWrite => LC_WRITE_BYTE
        }
    }
    
    fn from_value(value: u8) -> Result<Self, ParseError> where Self: std::marker::Sized {
        match value {
          BE_BYTE => Ok(RequestType::BeRead),
          LC_READ_BYTE => Ok(RequestType::LcRead),
          LC_WRITE_BYTE => Ok(RequestType::LcWrite),
          _ => Err(ParseError::InvalidMessageType(value))
        }
    }
    
    fn expected_len(&self) -> Option<usize> {
        match &self {
            RequestType::BeRead => None,
            RequestType::LcRead => Some(2*size_of::<u64>()),
            RequestType::LcWrite => None,
        }
    }

    fn iterator() -> impl Iterator<Item = RequestType> {
      [RequestType::BeRead, RequestType::LcRead, RequestType::LcWrite].iter().copied()
    }
}

struct MessageHeader {
  kind: RequestType,
  payload_len: usize,
}

impl MessageHeader {
  fn expected_len() -> usize {
    1 + LEN_LENGTH
  }

  fn len(&self) -> usize {
    MessageHeader::expected_len()
  }

  fn deserialize(packet: &[u8]) -> Result<MessageHeader,ParseError> {
    // Check for header (kind + Payload_len)
    check_length(packet.len(), MessageHeader::expected_len())?;
    
    let kind = RequestType::from_value(packet[0])?;
    let len: [u8; 8] = packet[1..(LEN_LENGTH + 1)].try_into().unwrap();
    let payload_len: usize = u64::from_be_bytes(len).try_into().unwrap();

    // If request has a specific length, validate
    if let Some(exp_len) = kind.expected_len() {
      if exp_len != payload_len {
        return Err(ParseError::UnexpectedLength { payload_len, exp_len });
      }
    }
    Ok(MessageHeader {
      kind, 
      payload_len
    })
  }
}

struct PayloadHeader {
  req_id: u64
}

impl PayloadHeader {
  fn expected_len() -> usize {
    LEN_LENGTH
  }

  fn len(&self) -> usize {
    PayloadHeader::expected_len()
  }

  fn deserialize(payload: &[u8]) -> Result<PayloadHeader, ParseError> {
    // payload should have at least the req_id bytes
    check_length(payload.len(), PayloadHeader::expected_len());
    let req_id_slice: [u8; 8] = payload[0..LEN_LENGTH].try_into().unwrap();
    let req_id = u64::from_be_bytes(req_id_slice);
    Ok(PayloadHeader { req_id })
  }
}

#[derive(Clone, Debug)]
pub enum Request {
  BeRead {
    req_id: u64,
    substring: String
  },
  LcRead {
    req_id: u64,
    id: u64
  },
  LcWrite {
    req_id: u64,
    id: u64,
    username: String
  }
}

impl Request {
  pub fn random(kind: RequestType, req_id: u64) -> Request {
    match kind {
        RequestType::BeRead => {
            Request::BeRead {
              req_id,
              substring: Alphanumeric.sample_string(&mut rand::rng(), SUBSTRING_LEN) 
            }
          },
        RequestType::LcRead => {
            Request::LcRead { 
              req_id,
              id: rand::rng().random_range(0..CAPACITY).try_into().unwrap()
            }
          }
        RequestType::LcWrite => {
            Request::LcWrite {
                req_id,
                id: rand::rng().random_range(0..CAPACITY).try_into().unwrap(),
                username: Alphanumeric.sample_string(&mut rand::rng(), rand::rng().random_range(0..10).try_into().unwrap()),
            }
        },
    }
  }
}

impl Message for Request {
  type Tag = RequestType;

  fn kind(&self) -> RequestType {
      match &self {
        Request::BeRead { .. } => RequestType::BeRead,
        Request::LcRead { .. } => RequestType::LcRead,
        Request::LcWrite { .. } => RequestType::LcWrite,
      }
  }

  fn serialize(&self) -> Vec<u8> {
    let mut packet: Vec<u8> = Vec::new();
    packet.push(self.kind().value());
    match self {
      Request::BeRead { substring, req_id } => {
        let mut payload: Vec<u8> = req_id.to_be_bytes().to_vec();
        payload.extend_from_slice(substring.as_bytes());
        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      },
      Request::LcRead { req_id, id } => {
        let mut payload: Vec<u8> = req_id.to_be_bytes().to_vec();
        payload.extend_from_slice(&id.to_be_bytes());
        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      },
      Request::LcWrite { req_id, id, username } => {
        let mut payload = req_id.to_be_bytes().to_vec();
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(username.as_bytes());
        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      }
    }
    packet
  }

  fn deserialize(packet: &[u8]) -> Result<Self, ParseError> {
    let header = MessageHeader::deserialize(&packet)?;
    
    // Check for payload length
    check_length(packet.len(), header.len() + header.payload_len)?;
    let payload = &packet[header.len()..(header.len() + header.payload_len)];
    let payload_header = PayloadHeader::deserialize(payload)?;
    
    let rest_payload = &payload[payload_header.len()..];
    match header.kind {
        RequestType::BeRead => {
          check_length(rest_payload.len(), 1);
          let str = String::from_utf8_lossy(rest_payload).to_string();
          Ok(Request::BeRead { req_id: payload_header.req_id, substring: str })
        },
        RequestType::LcRead => {
          let id = u64::from_be_bytes(rest_payload.try_into().unwrap()); // byte check already done
          Ok(Request::LcRead { req_id: payload_header.req_id, id })
        },
        RequestType::LcWrite => {
          check_length(rest_payload.len(), LEN_LENGTH + 1)?;
          let id = u64::from_be_bytes(rest_payload[0..LEN_LENGTH].try_into().unwrap());
          
          let uname_slice = &rest_payload[LEN_LENGTH..];
          let username = String::from_utf8_lossy(uname_slice).to_string();
          Ok(Request::LcWrite { req_id: payload_header.req_id, id, username })
        }
    }
  }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, Debug)]
pub enum ResponseType {
  BeRead,
  LcRead,
  LcWrite
}

impl MessageType for ResponseType {
  fn value(&self) -> u8 {
      match &self {
          ResponseType::BeRead => BE_BYTE,
          ResponseType::LcRead => LC_READ_BYTE,
          ResponseType::LcWrite => LC_WRITE_BYTE,
      }
  }
  
  fn from_value(value: u8) -> Result<Self, ParseError> where Self: std::marker::Sized {
      match value {
        BE_BYTE => Ok(ResponseType::BeRead),
        LC_READ_BYTE => Ok(ResponseType::LcRead),
        LC_WRITE_BYTE => Ok(ResponseType::LcWrite),
        _ => Err(ParseError::InvalidMessageType(value))
      }
  }
  
  fn expected_len(&self) -> Option<usize> {
      match &self {
        ResponseType::BeRead => Some(2*size_of::<u64>()),
        ResponseType::LcRead => None,
        ResponseType::LcWrite => None,
      }
  }

  fn iterator() -> impl Iterator<Item = ResponseType> {
    [ResponseType::BeRead, ResponseType::LcRead, ResponseType::LcWrite].iter().copied()
  }
}

impl ResponseType {
  pub fn from_request(req: RequestType) -> ResponseType {
    match req {
        RequestType::BeRead => ResponseType::BeRead,
        RequestType::LcRead => ResponseType::LcRead,
        RequestType::LcWrite => ResponseType::LcWrite,
    }
  }
}

#[derive(Clone, Debug)]
pub enum Response {
  BeRead {
    req_id: u64,
    freq: u64
  },
  LcRead {
    req_id: u64,
    username: Option<String>
  },
  LcWrite {
    req_id: u64,
    username: Option<String>
  }
}

impl Message for Response {
  type Tag = ResponseType;

  fn kind(&self) -> ResponseType {
      match &self {
        Response::BeRead { .. } => ResponseType::BeRead,
        Response::LcRead { .. } => ResponseType::LcRead,
        Response::LcWrite { .. } => ResponseType::LcWrite,
      }
  }

  fn serialize(&self) -> Vec<u8> {
    let mut packet: Vec<u8> = Vec::new();
    packet.push(self.kind().value());
    match self {
      Response::BeRead { req_id, freq } => {
        let mut payload = req_id.to_be_bytes().to_vec();
        payload.extend_from_slice(&freq.to_be_bytes());

        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      },
      Response::LcRead { req_id, username } | Response::LcWrite { req_id, username } => {
        let mut payload: Vec<u8> = req_id.to_be_bytes().to_vec();
        match username {
            Some(username) => {
              payload.push(SOME_BYTE);
              payload.extend_from_slice(username.as_bytes());
            },
            None => {
              payload.push(NONE_BYTE);
            },
        }
        packet.extend_from_slice(&payload.len().to_be_bytes());
        packet.extend_from_slice(&payload);
      }
    }
    packet
  }

  fn deserialize(packet: &[u8]) -> Result<Self, ParseError> {
    let header = MessageHeader::deserialize(&packet)?;
    
    // Check for payload length
    check_length(packet.len(), header.len() + header.payload_len)?;
    let payload = &packet[header.len()..(header.len() + header.payload_len)];
    let payload_header = PayloadHeader::deserialize(payload)?;
    
    let rest_payload = &payload[payload_header.len()..];
    let kind = ResponseType::from_request(header.kind);
    match kind {
      ResponseType::BeRead => {
          let freq = u64::from_be_bytes(rest_payload.try_into().unwrap()); // byte check already done
          Ok(Response::BeRead { req_id: payload_header.req_id, freq })
        },
        ResponseType::LcRead | ResponseType::LcWrite => {
          check_length(rest_payload.len(), 2)?;

          let res = match payload[0] {
            NONE_BYTE => None,
            SOME_BYTE => {
              let uname_slice = &rest_payload[1..];
              let username = String::from_utf8_lossy(uname_slice).to_string();
              Some(username)
            },
            _ => {return Err(ParseError::UnexpectedOptionType(payload[0]));}
          };

          match kind {
            ResponseType::LcRead => Ok(Response::LcRead { req_id: payload_header.req_id, username: res }),
            ResponseType::LcWrite => Ok(Response::LcWrite { req_id: payload_header.req_id, username: res }),
            _ => panic!("this should be impossible")
          }
        },
    }
  }
}

fn check_length(len: usize, exp: usize) -> Result<(), ParseError> {
  if len < exp {
    return Err(ParseError::PacketTooShort);
  } 
  Ok(())
}