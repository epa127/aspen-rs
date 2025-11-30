use rand::{Rng, distr::{Alphanumeric, SampleString}};
use crate::{BE_BYTE, CAPACITY, LC_READ_BYTE, LC_WRITE_BYTE, LEN_LENGTH, NONE_BYTE, SOME_BYTE, SUBSTRING_LEN};

pub trait Packet {
  type Tag: PacketType;
  fn kind(&self) -> Self::Tag;
  fn serialize(&self) -> Vec<u8>;
  fn deserialize(packet: &[u8]) -> Result<Self, String> where Self: std::marker::Sized;
}

pub trait PacketType {
  fn value(&self) -> u8;
  fn from_value(value: u8) -> Result<Self, String> where Self: std::marker::Sized;
  fn expected_len(&self) -> Option<usize>;
  fn iterator() -> impl Iterator<Item = Self>;
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, Debug)]
pub enum RequestType {
  BeRead,
  LcRead,
  LcWrite
}

impl PacketType for RequestType {
    fn value(&self) -> u8 {
        match &self {
            RequestType::BeRead => BE_BYTE,
            RequestType::LcRead => LC_READ_BYTE,
            RequestType::LcWrite => LC_WRITE_BYTE
        }
    }
    
    fn from_value(value: u8) -> Result<Self, String> where Self: std::marker::Sized {
        match value {
          BE_BYTE => Ok(RequestType::BeRead),
          LC_READ_BYTE => Ok(RequestType::LcRead),
          LC_WRITE_BYTE => Ok(RequestType::LcWrite),
          _ => Err("Value is not attributed to a request type.".to_string())
        }
    }
    
    fn expected_len(&self) -> Option<usize> {
        match &self {
            RequestType::BeRead => None,
            RequestType::LcRead => Some(size_of::<u64>()),
            RequestType::LcWrite => None,
        }
    }

    fn iterator() -> impl Iterator<Item = RequestType> {
      [RequestType::BeRead, RequestType::LcRead, RequestType::LcWrite].iter().copied()
    }
}

#[derive(Clone, Debug)]
pub enum Request {
  BeRead {
    substring: String
  },
  LcRead {
    id: u64
  },
  LcWrite {
    id: u64,
    username: String
  }
}

impl Request {
  pub fn random(kind: RequestType) -> Request {
    match kind {
        RequestType::BeRead => {
            Request::BeRead {
              substring: Alphanumeric.sample_string(&mut rand::rng(), SUBSTRING_LEN) 
            }
          },
        RequestType::LcRead => {
            Request::LcRead { 
              id: rand::rng().random_range(0..CAPACITY).try_into().unwrap()
            }
          }
        RequestType::LcWrite => {
            Request::LcWrite {
                id: rand::rng().random_range(0..CAPACITY).try_into().unwrap(),
                username: Alphanumeric.sample_string(&mut rand::rng(), rand::rng().random_range(0..10).try_into().unwrap()),
            }
        },
    }
  }
}

impl Packet for Request {
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
      Request::BeRead { substring } => {
        packet.extend_from_slice(&(substring.len() as u64).to_be_bytes());
        packet.extend_from_slice(substring.as_bytes());
      },
      Request::LcRead { id } => {
        let payload = id.to_be_bytes();
        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      },
      Request::LcWrite { id, username } => {
        let mut payload = id.to_be_bytes().to_vec();
        payload.extend_from_slice(username.as_bytes());
        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      }
    }
    packet
  }

  fn deserialize(packet: &[u8]) -> Result<Self, String> {
    let check_length = |len: usize, exp: usize| -> Result<(), String> {
      if len < exp {
        return Err("Packet too short".to_string());
      } 
      Ok(())
    };

    check_length(packet.len(), 1 + LEN_LENGTH)?;
    
    let kind = RequestType::from_value(packet[0])?;
    let len: [u8; 8] = packet[1..(LEN_LENGTH + 1)].try_into().unwrap();
    let payload_len: usize = u64::from_be_bytes(len).try_into()
      .map_err(|_| "Payload length was larger than expected.".to_string())?;

    if let Some(exp_len) = kind.expected_len() {
      if exp_len != payload_len {
        return Err(format!("Payload len {payload_len} not equal to expected len {exp_len}").to_string());
      }
    }

    check_length(packet.len(), 1 + LEN_LENGTH + payload_len)?;
    let payload = &packet[(LEN_LENGTH + 1)..(1 + LEN_LENGTH + payload_len)];
    
    match kind {
        RequestType::BeRead => {
          let str = String::from_utf8_lossy(payload).to_string();
          Ok(Request::BeRead { substring: str })
        },
        RequestType::LcRead => {
          let id = u64::from_be_bytes(payload.try_into().unwrap()); // byte check already done
          Ok(Request::LcRead { id })
        },
        RequestType::LcWrite => {
          check_length(payload_len, LEN_LENGTH)?;
          let id = u64::from_be_bytes(payload[0..LEN_LENGTH].try_into().unwrap());
          
          let uname_slice = &payload[LEN_LENGTH..payload_len];
          let username = String::from_utf8_lossy(uname_slice).to_string();
          Ok(Request::LcWrite { id, username })
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

impl PacketType for ResponseType {
  fn value(&self) -> u8 {
      match &self {
          ResponseType::BeRead => BE_BYTE,
          ResponseType::LcRead => LC_READ_BYTE,
          ResponseType::LcWrite => LC_WRITE_BYTE,
      }
  }
  
  fn from_value(value: u8) -> Result<Self, String> where Self: std::marker::Sized {
      match value {
        BE_BYTE => Ok(ResponseType::BeRead),
        LC_READ_BYTE => Ok(ResponseType::LcRead),
        LC_WRITE_BYTE => Ok(ResponseType::LcWrite),
        _ => Err("Value is not attributed to a request type.".to_string())
      }
  }
  
  fn expected_len(&self) -> Option<usize> {
      match &self {
        ResponseType::BeRead => Some(size_of::<u64>()),
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
    freq: u64
  },
  LcRead {
    username: Option<String>
  },
  LcWrite {
    username: Option<String>
  }
}

impl Packet for Response {
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
      Response::BeRead { freq } => {
        let payload = freq.to_be_bytes();
        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      },
      Response::LcRead { username } | Response::LcWrite { username } => {
        let mut payload: Vec<u8> = Vec::new();
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

  fn deserialize(packet: &[u8]) -> Result<Self, String> {
    let check_length = |len: usize, exp: usize| -> Result<(), String> {
      if len < exp {
        println!("BAD");
        return Err("Packet too short".to_string());
      } 
      Ok(())
    };

    const LEN_LENGTH: usize = size_of::<u64>();
    check_length(packet.len(), 1 + LEN_LENGTH)?;
    
    let kind = RequestType::from_value(packet[0])?;
    let len: [u8; 8] = packet[1..(LEN_LENGTH + 1)].try_into().unwrap();
    let payload_len: usize = u64::from_be_bytes(len).try_into()
      .map_err(|_| "Payload length was larger than expected.".to_string())?;

    if let Some(exp_len) = kind.expected_len() {
      if exp_len != payload_len {
        return Err(format!("Payload len {payload_len} not equal to expected len {exp_len}").to_string());
      }
    }

    check_length(packet.len(), 1 + LEN_LENGTH + payload_len)?;
    let payload = &packet[(LEN_LENGTH + 1)..(1 + LEN_LENGTH + payload_len)];
    
    match kind {
      RequestType::BeRead => {
          let freq = u64::from_be_bytes(payload.try_into().unwrap()); // byte check already done
          Ok(Response::BeRead { freq })
        },
        RequestType::LcRead | RequestType::LcWrite => {
          check_length(payload_len, 1)?;

          let res = match payload[0] {
            NONE_BYTE => None,
            SOME_BYTE => {
              let uname_slice = &payload[1..payload_len];
              let username = String::from_utf8_lossy(uname_slice).to_string();
              Some(username)
            },
            _ => {return Err("Unexpected byte in payload".to_string());}
          };

          match kind {
            RequestType::LcRead => Ok(Response::LcRead { username: res }),
            RequestType::LcWrite => Ok(Response::LcWrite { username: res }),
            _ => Err("Impossible match arm".to_string())
          }
        },
    }
  }
}