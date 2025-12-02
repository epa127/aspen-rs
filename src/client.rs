use std::{collections::HashMap, fs::{self, File}, io::{Read, Write}, net::TcpStream, thread::{self, JoinHandle}, time::Instant};
use hdrhistogram::Histogram;
use rand::Rng;

use crate::{BUF_LEN, LEN_LENGTH, SIG_FIG, packet::{Packet, PacketType, Request, RequestType, Response, ResponseType}};

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
    let header = format!("--- BENCHMARK TEST: {datetime} ---\n");
    
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

  fn send_packets(mut self) -> Result<Self, String> {
    let mut tasks_pending: usize = 0;
    while self.remaining_work > 0 || tasks_pending > 0 {
      let i = rand::rng().random_range(0..self.conns_per_thr);
      let conn = self.connections.get_mut(i).expect("Connection should exist.");
      // println!("Chose connection at {} with status {:?}", conn.stream.local_addr().unwrap().port(), conn.status);
      match conn.status.kind() {
        ConnStateType::Ready => {
          if self.remaining_work > 0 {
            let be_rat: f32 = rand::rng().random();
            let wr_rat: f32 = rand::rng().random();
            let req = if be_rat <= self.be_prob {
              Request::random(RequestType::BeRead)
            } else if wr_rat <= self.wr_lc_prob {
              Request::random(RequestType::LcWrite)
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
          if let Some((res_type, latency)) = conn.try_read()? {
            self.latencies.get_mut(&res_type).unwrap().push(latency);
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
  fn new(addr: &str) -> Result<Self, String> {
    match TcpStream::connect(addr) {
      Ok(stream) => { Ok(Connection { 
              stream, 
              status: ConnectionStatus::Ready 
            })}
      Err(e) => Err(e.to_string())
    }
  }

  fn send_initial_request(&mut self, req: Request) -> Result<(), String> {
    // println!("Sending {:?} from {}", req.kind(), self.stream.local_addr().unwrap().port());
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
      // println!("Resending {:?} from {}", req, self.stream.local_addr().unwrap().port());
      let req_bytes = write_buf.len();
      let bytes_written = self.stream.write(&write_buf[*offset..req_bytes]).map_err(|e| e.to_string())?;
      if bytes_written == req_bytes {
        self.status = ConnectionStatus::ReadingResponse { 
          exp_type: ResponseType::from_request(*req), 
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
        let _res = Response::deserialize(read_buf);
        // optional check for response
        let latency = start_time.elapsed().as_micros();
        self.status = ConnectionStatus::Ready;
        // println!("Response {:?} received from {} in {} µs", _res, self.stream.local_addr().unwrap(), latency);
        return Ok(Some((packet_type, latency)));
      } else {
        return Err(format!("Read more bytes than expected: {:?}", read_buf));
      }
    }
    Err("try_read() was called on a connection with invalid status.".to_string())
  }
}

#[derive(Clone, Debug)]
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