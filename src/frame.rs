use crate::{LEN_LENGTH, ParseError, packet::MessageType};

#[derive(Debug)]
pub struct ReadFrame<T: MessageType> {
  pub(crate) kind: Option<T>,
  pub(crate) read_buf: Vec<u8>,
  expected_len: Option<usize>,
}

impl<T: MessageType> Default for ReadFrame<T> {
  fn default() -> Self {
    Self::new()
  }
}

impl<T: MessageType> ReadFrame<T> {
  pub fn new() -> ReadFrame<T> {
    ReadFrame {
        kind: None,
        read_buf: Vec::new(),
        expected_len: None,
    }
  }

  pub fn push(&mut self, bytes: &[u8]) {
    self.read_buf.extend_from_slice(bytes);
  }

  pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, ParseError> {
    if self.read_buf.is_empty() {
      return Ok(None);
    }

    if self.kind.is_none() {
      self.kind = Some(T::from_value(self.read_buf[0])?);
    }

    // if self.kind.unwrap() == ResponseType::Drop {
      
    // }
    
    if self.expected_len.is_none() {
      if self.read_buf.len() < (1 + LEN_LENGTH) {
        return Ok(None);
      }

      let len_arr: [u8; 8] = self.read_buf[1..(1+LEN_LENGTH)].try_into().unwrap();
      self.expected_len = Some(usize::from_be_bytes(len_arr));
    }

    let total_exp_len = 1 + LEN_LENGTH + self.expected_len.unwrap();
    if self.read_buf.len() < total_exp_len {
      Ok(None)
    } else {
      let frame = self.read_buf[..total_exp_len].to_vec();
      self.read_buf.drain(..total_exp_len);
      Ok(Some(frame))
    }
  }

  pub fn reset(&mut self){
    self.kind = None;
    self.read_buf.clear();
    self.expected_len = None;
  }
}

#[derive(Debug)]
pub struct WriteFrame {
  buf: Vec<u8>,
  offset: usize,
}

impl WriteFrame {
  pub fn new(buf: Vec<u8>) -> Self {
      Self { buf, offset: 0 }
  }

  pub fn remaining(&self) -> &[u8] {
      &self.buf[self.offset..]
  }

  pub fn advance(&mut self, bytes_written: usize) -> WriteProgress {
      self.offset += bytes_written;
      if self.offset == self.buf.len() {
          WriteProgress::Done
      } else {
          WriteProgress::Partial
      }
  }
}

#[derive(PartialEq)]
pub enum WriteProgress {
  Partial,
  Done,
}


