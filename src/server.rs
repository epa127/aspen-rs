use std::{collections::HashMap, fs::File, net::SocketAddr, sync::{Arc, mpsc::SyncSender}};
use smol::{future::yield_now, io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, lock::RwLock};
use crate::{BUF_LEN, CAPACITY, LEN_LENGTH, packet::{Packet, PacketType, Request, RequestType, Response}};

const YIELD_FREQ: usize = 5; // yield every 2^n best effort sub-operations

struct Store {
  pub store: RwLock<HashMap<usize, String>>
}

impl Store {
  fn new() -> (Self, usize) {
    let file = File::open("bench/usernames.txt").unwrap();
    let mut rdr = csv::Reader::from_reader(file);
    let mut map: HashMap<usize, String> = HashMap::with_capacity(CAPACITY);
    for (i, result) in rdr.records().flatten().enumerate() {
      map.insert(i, result.iter().collect());
    }
    let len = map.len();
    
    (Store {
      store: RwLock::new(map)
    }, len)
  }

  async fn lc_read_task(&self, key: usize) -> Option<String> {
    self.store.read().await.get(&key).cloned()
  }

  async fn lc_write_task(&self, key: usize, value: String) -> Option<String> {
    self.store.write().await.insert(key, value)
  }

  async fn be_task(&self, substring: String) -> usize {
    let mut freq: usize = 0;

    let s = self.store.read().await;
    let e = s.clone();
    drop(s);

    for (i, username) in e.values().enumerate(){
      if username.contains(&substring) {
        freq += 1;
      }

      if (i & ((1 << YIELD_FREQ) - 1)) == 0 {
        yield_now().await;
      }
    }
    freq
  }
}

pub trait Server {
  fn init(port: usize, rx: SyncSender<()>);
}
pub struct DefaultSmolServer;

impl Server for DefaultSmolServer {
  fn init(port: usize, rx: SyncSender<()>) {
    println!("Building database...");
    let (store, store_len) = Store::new();
    let safe_store = Arc::new(store);
    println!("Successfully created database with {} keys. Starting TCP listener...", store_len);
    smol::block_on(async {
      let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await.unwrap();
      println!("TCP Listener bound to port {port}. Now accepting connections...");
      rx.send(()).unwrap();
      let mut i = true;
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
        smol::spawn(worker(stream, addr, store)).detach();
      }
    });
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
