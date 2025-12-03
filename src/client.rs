use std::{collections::HashMap, fs::{self, File}, io::{ErrorKind, Read, Write}, net::TcpStream, thread::{self, JoinHandle}, time::Instant};
use hdrhistogram::Histogram;
use rand::Rng;

use crate::{AspenRsError, BUF_LEN, LEN_LENGTH, NetworkError, ParseError, SIG_FIG, packet::{Message, MessageType, Request, RequestType, Response, ResponseType}};

#[derive(Debug)]
pub struct Client {
  be_lc_ratio: f32,
  conns_per_thr: usize,
  lc_write_read_ratio: f32,
  num_threads: usize,
  workload: usize,
}

impl Client {
  pub fn new(workload: usize, be_lc_ratio: f32, lc_write_read_ratio: f32, num_threads: usize, conns_per_thr: usize) -> Self {
    Client { 
      be_lc_ratio,
      conns_per_thr,
      lc_write_read_ratio,
      num_threads,
      workload,
    }
  }

  pub fn run(&self, port: usize) {
    let mut handles: Vec<JoinHandle<ClientThread>> = Vec::new();
    println!("Creating {} client threads", self.num_threads);
    for _ in 0..self.num_threads {
      let workload = self.workload / self.num_threads;
      let ratio = self.be_lc_ratio;
      let conns_per_thr = self.conns_per_thr;
      let wr_ratio = self.lc_write_read_ratio;
      handles.push(
        thread::spawn(move || {ClientThread::init(port, workload, ratio, conns_per_thr, wr_ratio)})
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

    let mut stat_map: HashMap<ResponseType, Histogram<u64>> = HashMap::new();
    for i in ResponseType::iterator() {
      stat_map.insert(i, Histogram::new_with_bounds(1, u64::MAX,SIG_FIG).unwrap());
    }

    for thr in client_threads {
      for (t, l) in thr.latencies {
        let hist = stat_map.get_mut(&t).unwrap();
        l.iter().for_each(|i| {let _ = hist.record(*i as u64);});
      }
    }

    self.general_results(tp_time, &stat_map);
    self.latency_by_quant_distr(&stat_map);
    
    println!("Completed benchmark!");
  }

  fn general_results(&self, total_secs: f32, stat_map: &HashMap<ResponseType, Histogram<u64>>) {
    let datetime = chrono::offset::Local::now();
    let header = format!("--- CLOSED-LOOP BENCHMARK TEST: {datetime} ---\n");
    
    let setup = format!("SETUP:\n    THREADS: {},\n    CONNECTIONS PER THREAD: {},\n    NUM TASKS: {}\n    BE:LC RATIO: {}\n    LC WRITE:READ RATIO: {}\n\n",
        self.num_threads, self.conns_per_thr, self.workload, self.be_lc_ratio, self.lc_write_read_ratio);
    let throughput = format!("THROUGHPUT: {} TASKS / {} SECONDS = {} TASKS PER SECOND\n\n", self.workload, total_secs, self.workload as f32 / total_secs);

    let mut stats = String::new();
    for t in ResponseType::iterator(){
      let hist = stat_map.get(&t).unwrap();
      let title = format!("{:?} STATS:\n", t);
      let size = format!("     SIZE: {}\n", hist.len());

      let vals = [
        hist.value_at_quantile(0.5) as f64,
        hist.value_at_quantile(0.95) as f64,
        hist.value_at_quantile(0.99) as f64,
        hist.value_at_quantile(0.999) as f64,
        hist.mean(),
        hist.stdev()
      ];

      let mut val_strs: Vec<String> = Vec::new();

      for val in vals {
        if val < 1e4 {
          // micros
          val_strs.push(format!("{} µs", val as u64));
        } else if val < 1e6 {
          // millis
          val_strs.push(format!("{:.3} ms", (val / 1000.0)));
        } else {
          // seconds
          val_strs.push(format!("{:.6} secs", (val / 1000000.0)));
        }
      }
      
      let median = format!("     p50 LATENCY: {}\n", val_strs[0]);
      let p95 = format!("     p95 LATENCY: {}\n", val_strs[1]);
      let p99 = format!("     p99 LATENCY: {}\n", val_strs[2]);
      let p999 = format!("     p99.9 LATENCY: {}\n", val_strs[3]);
      let mean = format!("     MEAN LATENCY: {}\n", val_strs[4]);
      let stddev = format!("     STD DEV: {}\n", val_strs[5]);

      stats = format!("{stats}{title}{size}{median}{p95}{p99}{p999}{mean}{stddev}\n");
    }

    // let data = format!("DATA:\n    BE DATA: {:?}\n    LC DATA: {:?}", be_agg, lc_agg);
    let prev = String::from_utf8_lossy(&fs::read("out/benchmark.txt").unwrap()).to_string();
    fs::write("out/benchmark.txt", format!("{header}{setup}{throughput}{stats}{prev}")).unwrap();
  }

  fn latency_by_quant_distr(&self, stat_map: &HashMap<ResponseType, Histogram<u64>>) {
    for (t, hist) in stat_map {
      let path = match t {
        ResponseType::BeRead => "beread",
        ResponseType::LcRead => "lcread",
        ResponseType::LcWrite => "lcwrite",
      };

      let file = File::open("bench/quantiles.txt").unwrap();
      let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
      let quantiles: Vec<f64> = rdr.records().map(
        |s| s.unwrap().get(0).unwrap().to_string().parse::<f64>().unwrap()).collect();
      
      let mut hist_data = format!("{:^8}    {:^8}    {:^8}    {:^8.3}\n", "Value", "Quantile", "Agg Count", "1/1-quantile");
      for quantile in quantiles {
        hist_data = format!("{hist_data}{:>8}    {:>8}    {:>8}    {:>8.3}\n",
         hist.value_at_quantile(quantile), quantile, (hist.len() as f64 * quantile) as u64, 1.0 / (1.0 - quantile));
      }
      let _ = fs::write(format!("out/{path}.txt"), hist_data);
    }
  }
}

struct ClientThread {
  conns_per_thr: usize,
  connections: Vec<Connection>,
  latencies: HashMap<ResponseType, Vec<u128>>,
  remaining_work: usize,
  be_prob: f32,
  wr_lc_prob: f32,
}

impl ClientThread {
  fn init(port: usize, workload: usize, be_prob: f32, conns_per_thr: usize, wr_lc_prob: f32) -> Self {
    let mut conns: Vec<Connection> = Vec::new();
    for _ in 0..conns_per_thr {
      conns.push(Connection::new(format!("127.0.0.1:{port}").as_str()).unwrap());
    }

    let mut latencies: HashMap<ResponseType, Vec<u128>> = HashMap::new();
    for t in ResponseType::iterator() {
      latencies.insert(t, Vec::new());
    }

    ClientThread {
      conns_per_thr,
      connections: conns,
      latencies,
      remaining_work: workload,
      be_prob,
      wr_lc_prob
    }
  }

  fn generate_random_request(&self) -> Request {
    let be_rat: f32 = rand::rng().random();
    let wr_rat: f32 = rand::rng().random();
    if be_rat <= self.be_prob {
      Request::random(RequestType::BeRead)
    } else if wr_rat <= self.wr_lc_prob {
      Request::random(RequestType::LcWrite)
    } else {
      Request::random(RequestType::LcRead)
    }
  }

  fn send_packets(mut self) -> Result<Self, AspenRsError> {
    let mut tasks_pending: usize = 0;
    let mut i = 0;
    while self.remaining_work > 0 || tasks_pending > 0 {
      
      // println!("Chose connection at {} with status {:?}", conn.stream.local_addr().unwrap().port(), conn.status);
      let req = {
        let conn = &self.connections[i];
        if conn.status.kind() == ConnStateType::Ready && self.remaining_work > 0 {
          self.remaining_work -= 1;
          Some(self.generate_random_request())
        } else {
          None
        }
      };

      let conn = &mut self.connections[i];

      match conn.progress(req)? {
        Progress::CompletedResponse(res_type, latency) => {
          self.latencies.get_mut(&res_type).unwrap().push(latency);
          tasks_pending -= 1;
        }
        Progress::ConnectionReset(in_flight) => {
          conn.reconnect()?;
          // if the connection was in-flight, adjust tasks_pending?
          if in_flight {
            tasks_pending -= 1;
          }
        },
        Progress::SentRequest => {
          tasks_pending += 1;
        }
        _ => {},
      }
      i = (i + 1) % self.conns_per_thr;
    }
    Ok(self)
  }
}

pub enum Progress {
  WouldBlock,
  MadeProgress,
  SentRequest,
  CompletedResponse(ResponseType, u128),
  ConnectionReset(bool), // in flight?
  Idle
}
struct Connection {
  stream: TcpStream,
  status: ConnectionStatus
}

impl Connection {
  fn new(addr: &str) -> Result<Self, NetworkError> {
    let stream = TcpStream::connect(addr)?;
    stream.set_nonblocking(true)?;
    Ok(Connection { 
      stream, 
      status: ConnectionStatus::Ready 
    })
  }

  fn reconnect(&mut self) -> Result<(), NetworkError> {
    let stream = TcpStream::connect(self.stream.local_addr()?)?;
    stream.set_nonblocking(true)?;
    self.stream = stream;
    self.status = ConnectionStatus::Ready;
    Ok(())
  }

  fn progress(&mut self, req: Option<Request>) -> Result<Progress, AspenRsError> {
    match &mut self.status {
        ConnectionStatus::Ready => {
          match req {
            Some(req) => {
              self.status = ConnectionStatus::WritingRequest { 
                req: req.kind(), 
                start_time: None, 
                write_buf: req.serialize(), 
                offset: 0 
              };
              Ok(Progress::MadeProgress)
            },
            None => Ok(Progress::Idle), 
          }
        },
        ConnectionStatus::WritingRequest { req, start_time, write_buf, offset } => {
          let req_bytes = write_buf.len();
          match self.stream.write(&write_buf[*offset..req_bytes]) {
            Ok(bytes_written) => {
              let was_started = start_time.is_some();
              if !was_started {
                  *start_time = Some(Instant::now());
              }
              if bytes_written == req_bytes {
                self.status = ConnectionStatus::ReadingResponse { 
                  exp_type: ResponseType::from_request(*req), 
                  start_time: (*start_time).unwrap(), 
                  read_buf: Vec::new(), 
                  expected_len: None  
                };
              } else {
                *offset += bytes_written;
              }
              if was_started {
                return Ok(Progress::MadeProgress);
              }
              Ok(Progress::SentRequest)
            },
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(Progress::WouldBlock),
            Err(e) if e.kind() == ErrorKind::ConnectionReset => Ok(Progress::ConnectionReset(start_time.is_some())),
            Err(e) => Err(AspenRsError::NetworkError(NetworkError::from(e)))
          }
        },
        ConnectionStatus::ReadingResponse { exp_type, start_time, read_buf, expected_len } => {
          let mut buf = [0; BUF_LEN];
          let check_type = read_buf.is_empty();
          match self.stream.read(&mut buf) {
            Ok(bytes_read) => {
              if bytes_read > 0 {
                read_buf.extend_from_slice(&buf[0..bytes_read]);
              } else {
                return Err(AspenRsError::NetworkError(NetworkError::ConnectionClosed));
              }
    
              let packet_type = ResponseType::from_value(*read_buf.first().unwrap())?;
              if check_type && *exp_type != packet_type {
                return Err(AspenRsError::ParseError(ParseError::UnexpectedMessageType{ exp_type: *exp_type, given_type: packet_type }));
              }
    
              if expected_len.is_none() {
                if read_buf.len() < (1 + LEN_LENGTH) {
                  return Ok(Progress::MadeProgress);
                }
    
                let len_arr: [u8; 8] = read_buf[1..(1+LEN_LENGTH)].try_into().unwrap();
                *expected_len = Some(usize::from_be_bytes(len_arr));
              }
    
              let total_exp_len = 1 + LEN_LENGTH + expected_len.expect("Should not be None based on previous checks");
    
              if read_buf.len() < total_exp_len {
                Ok(Progress::MadeProgress)
              } else if read_buf.len() == total_exp_len {
                let _res = Response::deserialize(read_buf).map_err(AspenRsError::ParseError)?;
                // optional check for response
                let latency = start_time.elapsed().as_micros();
                self.status = ConnectionStatus::Ready;
                // println!("Response {:?} received from {} in {} µs", _res, self.stream.local_addr().unwrap(), latency);
                return Ok(Progress::CompletedResponse(packet_type, latency));
              } else {
                println!("HERHE");
                return Err(AspenRsError::ParseError(ParseError::UnexpectedLength { payload_len: read_buf.len(), exp_len: total_exp_len }));
              }
            },
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(Progress::WouldBlock),
            Err(e) if e.kind() == ErrorKind::ConnectionReset => Ok(Progress::ConnectionReset(true)),
            Err(e) => Err(AspenRsError::NetworkError(NetworkError::from(e)))
          }   
        },
    }
  }
}

#[derive(Clone, Debug)]
enum ConnectionStatus {
  Ready,
  WritingRequest {
      req: RequestType,
      start_time: Option<Instant>,
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

#[derive(PartialEq, Eq)]
enum ConnStateType {
  Ready,
  WritingRequest,
  ReadingResponse
}