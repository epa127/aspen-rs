use std::{fs::{self}, io::{Read, Write}, net::TcpStream, thread::{self, JoinHandle}, time::Instant};
use hdrhistogram::Histogram;
use rand::Rng;

use crate::{BUF_LEN, LEN_LENGTH, SIG_FIG, packet::{Packet, PacketType, Request, RequestType, Response, ResponseType}};

pub struct Client {
  be_lc_ratio: f32,
  workload: usize,
  num_threads: usize,
  conns_per_thr: usize,
}

impl Client {
  pub fn new(workload: usize, be_lc_ratio: f32, num_threads: usize, conns_per_thr: usize) -> Self {
    Client { 
      be_lc_ratio, 
      workload,
      num_threads,
      conns_per_thr,
    }
  }

  pub fn run_benchmark(&self, port: usize) {
    let mut handles: Vec<JoinHandle<ClientThread>> = Vec::new();
    println!("Creating {} client threads", self.num_threads);
    for _ in 0..self.num_threads {
      let workload = self.workload / self.num_threads;
      let ratio = self.be_lc_ratio;
      let conns_per_thr = self.conns_per_thr;
      handles.push(
        thread::spawn(move || {ClientThread::init(port, workload, ratio, conns_per_thr)})
      );
    }

    let mut client_threads: Vec<ClientThread> = Vec::new();

    for handle in handles {
      client_threads.push(handle.join().unwrap());
    }

    println!("Begin sending requests...");
    let tp_timer = Instant::now();
    let mut handles: Vec<JoinHandle<ClientThread>> = Vec::new();
    for thread in client_threads {
      handles.push(
        thread::spawn(move || thread.send_packets().unwrap())
      );
    }

    let mut client_threads: Vec<ClientThread> = Vec::new();

    for handle in handles {
      client_threads.push(handle.join().unwrap());
    }

    let tp_time= tp_timer.elapsed().as_secs_f32();
    println!("All requests fulfilled in {tp_time} seconds! Calculating statistics...");

    let mut be_agg: Histogram<u64>= Histogram::new_with_bounds(1, u64::MAX,SIG_FIG).unwrap();
    let mut lc_agg: Histogram<u64>= Histogram::new_with_bounds(1, u64::MAX,SIG_FIG).unwrap();

    for thr in client_threads {
      thr.be_latencies.iter().for_each(|i| {let _ = be_agg.record(*i as u64);});
      thr.lc_latencies.iter().for_each(|i| {let _ = lc_agg.record(*i as u64);});
    }

    let datetime = chrono::offset::Local::now();
    let header = format!("--- BENCHMARK TEST: {datetime} ---\n");
    let setup = format!("SETUP:\n    THREADS: {},\n    CONNECTIONS PER THREAD: {},\n    NUM TASKS: {}\n    BE:LC RATIO: {} ({}:{})\n\n",
        self.num_threads, self.conns_per_thr, self.workload, self.be_lc_ratio, be_agg.len(), lc_agg.len());
    let throughput = format!("THROUGHPUT: {} TASKS / {} SECONDS = {} TASKS PER SECOND\n\n", self.workload, tp_time, self.workload as f32 / tp_time);
    let be_stats = format!("BE STATS:\n    p50 LATENCY: {} µs\n    p95 LATENCY: {} µs\n    p99 LATENCY: {} µs\n    p99.9 LATENCY: {} µs\n    MEAN LATENCY: {} µs\n    STD DEV: {} µs\n\n", 
        be_agg.value_at_quantile(0.5), lc_agg.value_at_quantile(0.95), be_agg.value_at_quantile(0.99), be_agg.value_at_quantile(0.999), be_agg.mean(), be_agg.stdev());
    let lc_stats = format!("LC STATS:\n    p50 LATENCY: {} µs\n    p95 LATENCY: {} µs\n    p99 LATENCY: {} µs\n    p99.9 LATENCY: {} µs\n    MEAN LATENCY: {} µs\n    STD DEV: {} µs\n\n", 
        lc_agg.value_at_quantile(0.5), lc_agg.value_at_quantile(0.95), lc_agg.value_at_quantile(0.99), lc_agg.value_at_quantile(0.999), lc_agg.mean(), lc_agg.stdev());
    // let data = format!("DATA:\n    BE DATA: {:?}\n    LC DATA: {:?}", be_agg, lc_agg);
    let prev = String::from_utf8_lossy(&fs::read("out/benchmark.txt").unwrap()).to_string();
    fs::write("out/benchmark.txt", format!("{header}{setup}{throughput}{be_stats}{lc_stats}{prev}")).unwrap();
    
    println!("Completed benchmark!");
  }
}

struct ClientThread {
  conns_per_thr: usize,
  connections: Vec<Connection>,
  be_latencies: Vec<u128>,
  lc_latencies: Vec<u128>,
  remaining_work: usize,
  be_prob: f32,
}

impl ClientThread {
  fn init(port: usize, workload: usize, be_prob: f32, conns_per_thr: usize) -> Self {
    let mut conns: Vec<Connection> = Vec::new();
    for _ in 0..conns_per_thr {
      conns.push(Connection::new(format!("127.0.0.1:{port}").as_str()).unwrap());
    }

    ClientThread {
      conns_per_thr,
      connections: conns,
      be_latencies: Vec::new(),
      lc_latencies: Vec::new(),
      remaining_work: workload,
      be_prob
    }
  }

  fn send_packets(mut self) -> Result<Self, String> {
    let mut tasks_pending: usize = 0;
    while self.remaining_work > 0 || tasks_pending > 0 {
      let i = rand::rng().random_range(0..self.conns_per_thr);
      let conn = self.connections.get_mut(i).expect("Connection should exist.");
      match conn.status.kind() {
        ConnStateType::Ready => {
          if self.remaining_work > 0 {
            let be_rat: f32 = rand::rng().random();
            let req = if be_rat <= self.be_prob {
              Request::random(RequestType::BeRead)
            } else {
              Request::random(RequestType::LcRead)
            };
            conn.send_initial_request(req)?;
            self.remaining_work -= 1;
            tasks_pending += 1;
          }
        },
        ConnStateType::WritingRequest => {
          conn.resend_request()?;
        },
        ConnStateType::ReadingResponse => {
          if let Ok(Some((res_type, latency))) = conn.try_read() {
            match res_type {
              ResponseType::BestEffort => {self.be_latencies.push(latency);},
              ResponseType::LatencyCritical => {self.lc_latencies.push(latency);},
            }
            tasks_pending -= 1;
          }
        }
      }
    }
    Ok(self)
  }
}

struct Connection {
  stream: TcpStream,
  status: ConnectionStatus
}

impl Connection {
  fn new(addr: &str) -> Result<Self, ()> {
    if let Ok(stream) = TcpStream::connect(addr) {
      return Ok(Connection { 
        stream, 
        status: ConnectionStatus::Ready 
      });
    } 
    Err(())
  }

  fn send_initial_request(&mut self, req: Request) -> Result<(), String> {
    let request= req.serialize();
    let req_bytes = request.len();
    let start_time = Instant::now();
    let bytes_written = self.stream.write(&request).map_err(|e| e.to_string())?;
    self.status = if bytes_written == req_bytes {
      // println!("Request sent to {}", self.stream.local_addr().unwrap());
      ConnectionStatus::ReadingResponse { 
        exp_type: ResponseType::from_request(req.kind()), 
        start_time, 
        read_buf: Vec::new(), 
        expected_len: None 
      }
    } else {
      ConnectionStatus::WritingRequest { 
        req: req.kind(), 
        start_time, 
        write_buf: request, 
        offset: bytes_written 
      }
    };
    Ok(())
  }

  fn resend_request(&mut self) -> Result<(), String> {
    if let ConnectionStatus::WritingRequest { req, start_time, write_buf, offset } = &mut self.status {
      let req_bytes = write_buf.len();
      let bytes_written = self.stream.write(&write_buf[*offset..req_bytes]).map_err(|e| e.to_string())?;
      if bytes_written == req_bytes {
        self.status = ConnectionStatus::ReadingResponse { 
          exp_type: ResponseType::from_request(req.clone()), 
          start_time: *start_time, 
          read_buf: Vec::new(), 
          expected_len: None 
        };
      } else {
        *offset += bytes_written;
      }
      return Ok(());
    }
    Err("Resend request was called with an invalid connection status.".to_string())
  }

  /// Returns Ok(Some((RequestType, latency)) if read is complete, Ok(None) for a partial read, and Errs otherwise.
  fn try_read(&mut self) -> Result<Option<(ResponseType, u128)>, String> {
    if let ConnectionStatus::ReadingResponse { exp_type, start_time, read_buf, expected_len } = &mut self.status {
      let mut buf = [0; BUF_LEN];
      let check_type = read_buf.is_empty();
      let bytes_read = self.stream.read(&mut buf).map_err(|e| e.to_string())?;
      if bytes_read > 0 {
        read_buf.extend_from_slice(&buf[0..bytes_read]);
      } else {
        return Ok(None);
      }

      let packet_type = ResponseType::from_value(*read_buf.first().unwrap())?;
      if check_type && *exp_type != packet_type {
        return Err("Connection received packet of unexpected type.".to_string());
      }

      if expected_len.is_none() {
        if read_buf.len() < (1 + LEN_LENGTH) {
          return Ok(None);
        }

        let len_arr: [u8; 8] = read_buf[1..(1+LEN_LENGTH)].try_into().map_err(|e: std::array::TryFromSliceError| e.to_string())?;
        *expected_len = Some(usize::from_be_bytes(len_arr));
      }

      let total_exp_len = 1 + LEN_LENGTH + expected_len.expect("Should not be None based on previous checks");
      
      if read_buf.len() < total_exp_len {
        return Ok(None);
      } else if read_buf.len() == total_exp_len {
        let _res = Response::deserialize(read_buf)?;
        // optional check for response
        let latency = start_time.elapsed().as_micros();
        self.status = ConnectionStatus::Ready;
        // println!("Response {:?} received from {} in {} µs", res, self.stream.local_addr().unwrap(), latency);
        return Ok(Some((packet_type, latency)));
      } else {
        return Err(format!("Read more bytes than expected: {:?}", read_buf));
      }
    }
    Err("try_read() was called on a connection with invalid status.".to_string())
  }
}

#[derive(Clone)]
enum ConnectionStatus {
  Ready,
  WritingRequest {
      req: RequestType,
      start_time: Instant,
      write_buf: Vec<u8>,
      offset: usize, // start writing at this value
  },
  ReadingResponse {
      exp_type: ResponseType,
      start_time: Instant,
      read_buf: Vec<u8>,
      expected_len: Option<usize>,
  }
}

impl ConnectionStatus {
  fn kind(&self) -> ConnStateType {
    match &self {
        ConnectionStatus::Ready => ConnStateType::Ready,
        ConnectionStatus::WritingRequest { .. } => ConnStateType::WritingRequest,
        ConnectionStatus::ReadingResponse { .. } => ConnStateType::ReadingResponse,
    }
  }
}

enum ConnStateType {
  Ready,
  WritingRequest,
  ReadingResponse
}