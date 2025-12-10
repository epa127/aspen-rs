use std::{collections::VecDeque, net::SocketAddr, sync::{Arc, mpsc::SyncSender}};
use smol::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use crate::{AspenRsError, BUF_LEN, LEN_LENGTH, NetworkError, frame::{ReadFrame, WriteFrame, WriteProgress}, packet::{Message, MessageType, Request, RequestType, Response}, store::Store};


use async_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded, unbounded};
use async_executor::Executor;
use easy_parallel::Parallel;
use futures_lite::future;

pub struct DefaultSmolServer;

impl DefaultSmolServer {
  pub fn init(num_threads: usize, port: usize, start_client: SyncSender<()>, database: Store, num_workers: usize, queue_size: usize) {
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
          let (global_tx, global_rx) = bounded::<Job>(queue_size);
          
          ex.run(async move {
            for _ in 0..num_workers {
              let store = safe_store.clone();
              let mut worker = Worker::new(global_rx.clone(), store);
              ex_clone.spawn(async move {
                worker.run().await.unwrap();
              }).detach();
            }

            println!("TCP Listener bound to port {port}. Now accepting connections...");
            start_client.send(()).unwrap();
            loop {
              async fn io_thread(stream: TcpStream, addr: SocketAddr, global_tx: Sender<Job>) {
                match IoThread::new(stream, addr, global_tx).run().await {
                    Ok(_) | Err(AspenRsError::NetworkError(NetworkError::ConnectionReset)) => {},
                    Err(e) => eprintln!("{e}"),
                }
              }

              let (stream, addr) = listener.accept().await.unwrap();

              if i {
                println!("Server accepted first connection at addr {:?}. Now spawning workers...", addr);
                i = false;
              }
              ex_clone.spawn(io_thread(stream, addr, global_tx.clone())).detach();
            }
          }).await;
          drop(signal);
        }));
  }
}

struct IoThread {
  stream: TcpStream,
  _addr: SocketAddr,

  global_tx: Sender<Job>, // Sends job requests to worker threads
  tx_resp: Sender<Response>, // Attaches to each job request for return_sends
  rx_resp: Receiver<Response>, // Receives responses from workers

  write_queue: VecDeque<WriteFrame>
}

impl IoThread {
  fn new(stream: TcpStream, addr: SocketAddr, global_tx: Sender<Job>) -> Self {
    let (tx_resp, rx_resp)= unbounded::<Response>();
    IoThread {
        stream,
        _addr: addr,
        global_tx,
        tx_resp,
        rx_resp,
        write_queue: VecDeque::new(),
    }
  }

  async fn run(&mut self) -> Result<(), AspenRsError> {
    loop {
      self.refresh_queue()?;
      self.receive_requests().await?;
      self.send_responses().await?;
    }
  }
  
  async fn receive_requests(&mut self) -> Result<(), AspenRsError> {
    let mut frame = ReadFrame::<RequestType>::new();
    let mut buf = vec![0u8; BUF_LEN];

    let bytes_read = self.stream.read(&mut buf).await.map_err(|e| AspenRsError::NetworkError(NetworkError::from(e)))?;
    if bytes_read > 0 {
      frame.push(&buf[0..bytes_read]);
    } else {
      return Err(AspenRsError::NetworkError(NetworkError::ConnectionClosed));
    }

    while let Some(req_buf) = frame.next_frame()? {
      let req = Request::deserialize(&req_buf).map_err(AspenRsError::ParseError)?;
      let req_id = req.get_id();
      let job = Job::new(req, self.tx_resp.clone());

      if let Err(e) = self.global_tx.try_send(job) {
        match e {
          TrySendError::Full(_) => {
            let drop_res = Response::Drop { req_id };
            let frame = WriteFrame::new(Response::serialize(&drop_res));
            self.write_queue.push_back(frame);
          },
          TrySendError::Closed(_) => return Err(AspenRsError::InternalError("async global receive buffer closed".to_string())),
        }
      }
    }
    Ok(())
  }

  fn refresh_queue(&mut self) -> Result<(), AspenRsError> {
    loop {
      match self.rx_resp.try_recv() {
        Ok(res) => {
          self.write_queue.push_back(WriteFrame::new(Response::serialize(&res)));
        },
        Err(e) => match e {
            TryRecvError::Empty => break,
            TryRecvError::Closed => return Err(AspenRsError::InternalError("async global receive buffer closed".to_string())),
        },
      }
    }
    Ok(())
  }

  async fn send_responses(&mut self) -> Result<(), AspenRsError> {
    while let Some(write_frame) = self.write_queue.front_mut() {
      match self.stream.write(write_frame.remaining()).await {
        Ok(bytes_written) => {
            match write_frame.advance(bytes_written) {
                WriteProgress::Partial => break,
                WriteProgress::Done => { self.write_queue.pop_front().unwrap(); },
            }
        },
        Err(e) => return Err(AspenRsError::NetworkError(NetworkError::from(e))),
      }
    }
    Ok(())
  }
}

struct Job {
  req: Request,
  return_send: Sender<Response>
}

impl Job {
  fn new(req: Request, return_send: Sender<Response>) -> Self {
    Job {
        req,
        return_send,
    }
  }
}

struct Worker {
  global_rx: Receiver<Job>,
  store: Arc<Store>,
}

impl Worker {
  fn new(global_rx: Receiver<Job>, store: Arc<Store>) -> Self {
    Worker {
      global_rx,
      store
    }
  }

  async fn run(&mut self) -> Result<(), AspenRsError> {
    loop {
      let job = self.global_rx.recv().await.map_err(|e| AspenRsError::InternalError(e.to_string()))?;
      let res = self.execute_task(job.req).await;
      job.return_send.send(res).await.map_err(|e| AspenRsError::InternalError(e.to_string()))?;
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
}
