use std::{net::SocketAddr, str::FromStr, sync::{Arc, atomic::{AtomicUsize, Ordering}, mpsc::SyncSender}, thread};
use smol::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use socket2::{Domain, SockAddr, Socket, Type};
use crate::{BACKLOG, BUF_LEN, LEN_LENGTH, packet::{Packet, PacketType, Request, RequestType, Response}, store::{self, Store}};

pub struct DefaultSmolServer;

impl DefaultSmolServer {
  pub fn init(num_threads: usize, port: usize, start_client: SyncSender<()>, database: Store) {
    let safe_store = Arc::new(database);
    let ready_count = Arc::new(AtomicUsize::new(0));

    for i in 0..num_threads {
      let store_clone = safe_store.clone();
      let rx_clone = start_client.clone();
      let ready_count_clone = ready_count.clone();
      thread::spawn( move ||
        smol::block_on(async {
          let socket = Socket::new(Domain::IPV4, Type::STREAM, None).map_err(|e| e.to_string()).unwrap();
          socket.set_reuse_port(true).map_err(|e| e.to_string()).unwrap();
          let addr = SockAddr::from(SocketAddr::from_str(format!("127.0.0.1:{port}").as_str())
              .map_err(|e| e.to_string()).unwrap());
          socket.bind(&addr).map_err(|e| e.to_string()).unwrap();
          socket.listen(BACKLOG).map_err(|e| e.to_string()).unwrap();
          println!("TCP Listener bound to port {port}. Now accepting connections...");

          let listener: std::net::TcpListener = socket.into();
          let async_listener = TcpListener::try_from(listener).unwrap(); 
          let prev = ready_count_clone.fetch_add(1, Ordering::SeqCst);
          if prev + 1 == num_threads {
              // this is the LAST thread to bind
              rx_clone.send(()).unwrap();
          }
          let mut seen = true;
          loop {
            let store = store_clone.clone();
            let (stream, addr) = async_listener.accept().await.unwrap();
            async fn worker(stream: TcpStream, addr: SocketAddr, store: Arc<Store>) {
              Worker::new(stream, addr, store.clone()).run().await;
            }
            if seen {
              println!("Server {i} accepted first connection at addr {:?}. Now spawning workers...", addr);
              seen = false;
            }
            smol::spawn(worker(stream, addr, store)).detach();
          }
        })
      );
    }
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
