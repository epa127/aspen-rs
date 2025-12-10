use std::{net::SocketAddr, sync::{Arc, mpsc::SyncSender}};
use smol::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use crate::{AspenRsError, BUF_LEN, LEN_LENGTH, NetworkError, frame::{ReadFrame, WriteFrame, WriteProgress}, packet::{Message, MessageType, Request, RequestType, Response}, store::Store};


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
                match Worker::new(stream, addr, store.clone()).run().await {
                    Ok(_) | Err(AspenRsError::NetworkError(NetworkError::ConnectionReset)) => {},
                    Err(e) => eprintln!("{e}"),
                }
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

  async fn run(mut self) -> Result<(), AspenRsError> {
    loop {
      let req = self.receive_request().await?;
      let res = self.execute_task(req).await;
      self.send_response(res).await?;
    }
  }
  
  async fn receive_request(&mut self) -> Result<Request, AspenRsError> {
    let mut frame = ReadFrame::<RequestType>::new();
    let mut buf = vec![0u8; BUF_LEN];

    loop {
      let bytes_read = self.stream.read(&mut buf).await.map_err(|e| AspenRsError::NetworkError(NetworkError::from(e)))?;
      if bytes_read > 0 {
        frame.push(&buf[0..bytes_read]);
      } else {
        return Err(AspenRsError::NetworkError(NetworkError::ConnectionClosed));
      }

      match frame.next_frame()? {
        Some(req) => return Request::deserialize(&req).map_err(AspenRsError::ParseError),
        None => continue,
      }
    }
  }

  async fn execute_task(&mut self, req: Request) -> Response {
    match req {
        Request::BeRead { req_id, substring } => {
            let freq: u64 = self.store.be_task(substring).await as u64;
            Response::BeRead { req_id, freq }
          },
        Request::LcRead { req_id, id } => {
            let id = id.try_into().unwrap();
            let username = self.store.lc_read_task(id).await;
            Response::LcRead { req_id, username }
          },
        Request::LcWrite { req_id, id, username } => {
            let id = id.try_into().unwrap();
            let username = self.store.lc_write_task(id, username).await;
            Response::LcWrite { req_id, username }
        },
    }
  }

  async fn send_response(&mut self, res: Response) -> Result<(), AspenRsError> {
    let mut write_frame = WriteFrame::new(res.serialize());
    loop {
      match self.stream.write(write_frame.remaining()).await {
        Ok(bytes_written) => {
            match write_frame.advance(bytes_written) {
                WriteProgress::Partial => continue,
                WriteProgress::Done => return Ok(()),
            }
        },
        Err(e) => return Err(AspenRsError::NetworkError(NetworkError::from(e))),
      }
    }
  }
}
