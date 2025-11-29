use rand::{Rng, distr::{Alphanumeric, SampleString}};
use crate::{BE_BYTE, CAPACITY, LC_BYTE, LEN_LENGTH, SUBSTRING_LEN};

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
}

#[derive(Clone)]
pub enum RequestType {
  BeRead,
  LcRead
}

impl PacketType for RequestType {
    fn value(&self) -> u8 {
        match &self {
            RequestType::BeRead => BE_BYTE,
            RequestType::LcRead => LC_BYTE,
        }
    }
    
    fn from_value(value: u8) -> Result<Self, String> where Self: std::marker::Sized {
        match value {
          BE_BYTE => Ok(RequestType::BeRead),
          LC_BYTE => Ok(RequestType::LcRead),
          _ => Err("Value is not attributed to a request type.".to_string())
        }
    }
    
    fn expected_len(&self) -> Option<usize> {
        match &self {
            RequestType::BeRead => None,
            RequestType::LcRead => Some(size_of::<u64>()),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Request {
  BeRead {
    substring: String
  },
  LcRead {
    id: u64
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
    }
  }
}

impl Packet for Request {
  type Tag = RequestType;

  fn kind(&self) -> RequestType {
      match &self {
        Request::BeRead { .. } => RequestType::BeRead,
        Request::LcRead { .. } => RequestType::LcRead,
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
    }
  }
}

#[derive(Clone, PartialEq)]
pub enum ResponseType {
  BestEffort,
  LatencyCritical
}

impl PacketType for ResponseType {
  fn value(&self) -> u8 {
      match &self {
          ResponseType::BestEffort => BE_BYTE,
          ResponseType::LatencyCritical => LC_BYTE,
      }
  }
  
  fn from_value(value: u8) -> Result<Self, String> where Self: std::marker::Sized {
      match value {
        BE_BYTE => Ok(ResponseType::BestEffort),
        LC_BYTE => Ok(ResponseType::LatencyCritical),
        _ => Err("Value is not attributed to a request type.".to_string())
      }
  }
  
  fn expected_len(&self) -> Option<usize> {
      match &self {
        ResponseType::BestEffort => Some(size_of::<u64>()),
        ResponseType::LatencyCritical => None,
      }
  }
}

impl ResponseType {
  pub fn from_request(req: RequestType) -> ResponseType {
    match req {
        RequestType::BeRead => ResponseType::BestEffort,
        RequestType::LcRead => ResponseType::LatencyCritical,
    }
  }
}

#[derive(Clone, Debug)]
pub enum Response {
  BestEffort {
    freq: u64
  },
  LatencyCritical {
    username: String
  }
}

impl Packet for Response {
  type Tag = ResponseType;

  fn kind(&self) -> ResponseType {
      match &self {
        Response::BestEffort { .. } => ResponseType::BestEffort,
        Response::LatencyCritical { .. } => ResponseType::LatencyCritical,
      }
  }

  fn serialize(&self) -> Vec<u8> {
    let mut packet: Vec<u8> = Vec::new();
    packet.push(self.kind().value());
    match self {
      Response::BestEffort { freq } => {
        let payload = freq.to_be_bytes();
        packet.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        packet.extend_from_slice(&payload);
      },
      Response::LatencyCritical { username } => {
        packet.extend_from_slice(&(username.len() as u64).to_be_bytes());
        packet.extend_from_slice(username.as_bytes());
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

    const LEN_LENGTH: usize = size_of::<u64>();
    check_length(packet.len(), 1 + LEN_LENGTH)?;
    
    let kind = ResponseType::from_value(packet[0])?;
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
        ResponseType::LatencyCritical => {
          let str = String::from_utf8_lossy(payload).to_string();
          Ok(Response::LatencyCritical { username: str })
        },
        ResponseType::BestEffort => {
          let freq = u64::from_be_bytes(payload.try_into().unwrap()); // byte check already done
          Ok(Response::BestEffort { freq })
        }
    }
  }
}