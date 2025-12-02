use std::{net::SocketAddr, sync::{Arc, mpsc::SyncSender}};
use smol::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use crate::{BUF_LEN, LEN_LENGTH, packet::{Packet, PacketType, Request, RequestType, Response}, store::Store};


use async_channel::unbounded;
use async_executor::Executor;
use easy_parallel::Parallel;
use futures_lite::future;

pub struct DefaultSmolServer;

impl DefaultSmolServer {
  pub fn init(num_threads: usize, port: usize, start_client: SyncSender<()>, database: Store) {
    let safe_store = Arc::new(database);

    let ex = Arc::new(Executor::new());
    let (signal, shutdown) = unbounded::<()>();

    Parallel::new()
        // Run four executor threads.
        .each(0..num_threads, |_| future::block_on(ex.run(shutdown.recv())))
        // Run the main future on the current thread.
        .finish(|| future::block_on(async {
          let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await.unwrap();
          let mut i = true;
          let ex_clone = ex.clone();
          ex.run(async move {
            println!("TCP Listener bound to port {port}. Now accepting connections...");
            start_client.send(()).unwrap();
            loop {
              let store = safe_store.clone();
              let (stream, addr) = listener.accept().await.unwrap();
              async fn worker(stream: TcpStream, addr: SocketAddr, store: Arc<Store>) {
                Worker::new(stream, addr, store.clone()).run().await;
              }
              if i {
                println!("Server accepted first connection at addr {:?}. Now spawning workers...", addr);
                i = false;
              }
              ex_clone.spawn(worker(stream, addr, store)).detach();
            }
          }).await;
          drop(signal);
        }));
  }
}

struct Worker {
  stream: TcpStream,
  _addr: SocketAddr,
  store: Arc<Store>,
}

impl Worker {
  fn new(stream: TcpStream, addr: SocketAddr, store: Arc<Store>) -> Self {
    Worker {
      stream,
      _addr: addr, 
      store
    }
  }

  async fn run(mut self) {
    loop {
      let req = self.receive_request().await.unwrap();
      let res = self.execute_task(req).await.unwrap();
      self.send_response(res).await.unwrap();
    }
  }
  
  async fn receive_request(&mut self) -> Result<Request, String> {
    let mut read_buf: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; BUF_LEN];
    let mut req_type: Option<RequestType> = None;
    let mut expected_len: Option<usize> = None;

    loop {
      let bytes_read = self.stream.read(&mut buf).await.map_err(|e| e.to_string())?;
      if bytes_read > 0 {
        read_buf.extend_from_slice(&buf[0..bytes_read]);
      } else {
        continue;
      }

      if req_type.is_none() {
        req_type = Some(RequestType::from_value(read_buf[0])?);
      }
      
      if expected_len.is_none() {
        if read_buf.len() < (1 + LEN_LENGTH) {
          continue;
        }

        let len_arr: [u8; 8] = read_buf[1..(1+LEN_LENGTH)].try_into().map_err(|e: std::array::TryFromSliceError| e.to_string())?;
        expected_len = Some(usize::from_be_bytes(len_arr));
      }

      let total_exp_len = 1 + LEN_LENGTH + expected_len.expect("Should not be None based on previous checks");
      
      if read_buf.len() < total_exp_len {
        continue
      } else if read_buf.len() == total_exp_len {
        return Request::deserialize(&read_buf);
      } else {
        return Err(format!("Read more bytes than expected: {:?}", read_buf));
      }
    }
  }

  async fn execute_task(&mut self, req: Request) -> Result<Response, String> {
    match req {
        Request::BeRead { substring } => {
            let freq: u64 = self.store.be_task(substring).await as u64;
            Ok(Response::BeRead { freq })
          },
        Request::LcRead { id } => {
            let id = id.try_into().map_err(|e: std::num::TryFromIntError| e.to_string())?;
            let username = self.store.lc_read_task(id).await;
            Ok(Response::LcRead { username })
          },
        Request::LcWrite { id, username } => {
            let id = id.try_into().map_err(|e: std::num::TryFromIntError| e.to_string())?;
            let username = self.store.lc_write_task(id, username).await;
            Ok(Response::LcWrite { username })
        },
    }
  }

  async fn send_response(&mut self, res: Response) -> Result<(), String> {
    let response = res.serialize();
    let res_bytes = response.len();
    let mut offset: usize = 0;

    loop {
      let bytes_written = self.stream.write(&response[offset..res_bytes]).await.map_err(|e| e.to_string())?;
      if bytes_written == res_bytes {
        return Ok(());
      } else {
        offset += bytes_written;
      }
    }
  }
}
