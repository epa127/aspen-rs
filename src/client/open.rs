use std::{collections::{HashMap, VecDeque}, fs::{self, File}, io::{ErrorKind, Read, Write}, net::TcpStream, thread::{self, JoinHandle}, time::Instant};

use hdrhistogram::Histogram;
use rand::Rng;
use rand_distr::{Distribution, Exp};
use crate::{AspenRsError, BUF_LEN, LEN_LENGTH, NetworkError, ParseError, SIG_FIG, packet::{Message, MessageType, Request, RequestType, Response, ResponseType}};


pub struct OpenBench {
  target_rps: u64,
  runtime_secs: f32,
  be_lc_ratio: f32,
  lc_wr_ratio: f32,
  num_threads: usize,
  conns_per_thr: usize,
}

impl OpenBench {
  pub fn new(target_rps: u64,
    runtime_secs: f32,
    be_lc_ratio: f32,
    lc_wr_ratio: f32,
    num_threads: usize,
    conns_per_thr: usize) -> Self {
    OpenBench { target_rps, runtime_secs, be_lc_ratio, lc_wr_ratio, num_threads, conns_per_thr }
  }

  pub fn run(&self, port: usize) {
    let mut handles: Vec<JoinHandle<ClientThread>> = Vec::new();
    println!("Creating {} client threads", self.num_threads);
    for i in 0..self.num_threads {
      let be_prob = self.be_lc_ratio;
      let conns_per_thr = self.conns_per_thr;
      let lc_wr_prob = self.lc_wr_ratio;
      let shift: u8 = (usize::BITS - self.num_threads.leading_zeros()).try_into().unwrap();
      let rps = self.target_rps;
      handles.push(
        thread::spawn(move || {
          ClientThread::init(port,conns_per_thr,be_prob,
            lc_wr_prob,i as u64,shift,rps)
        })
      );
    }

    let mut client_threads: Vec<ClientThread> = Vec::new();

    for handle in handles {
      client_threads.push(handle.join().unwrap());
    }

    println!("Begin sending requests...");
    let mut handles: Vec<JoinHandle<ClientThread>> = Vec::new();
    for thread in client_threads {
      let runtime = self.runtime_secs;
      handles.push(
        thread::spawn(move || thread.send_packets(runtime).unwrap())
      );
    }

    let mut client_threads: Vec<ClientThread> = Vec::new();

    for handle in handles {
      client_threads.push(handle.join().unwrap());
    }

    let mut stat_map: HashMap<ResponseType, Histogram<u64>> = HashMap::new();
    for i in ResponseType::iterator() {
      stat_map.insert(i, Histogram::new_with_bounds(1, u64::MAX,SIG_FIG).unwrap());
    }

    let mut drop_count = 0u64;
    let mut reqs = 0u64;
    for thr in client_threads {
      for (t, l) in thr.latencies {
        let hist = stat_map.get_mut(&t).unwrap();
        l.iter().for_each(|i| {let _ = hist.record(*i as u64);});
      }

      drop_count += thr.drop_count;
      reqs += thr.req_id >> thr.req_id_shift;

      self.general_results(reqs, drop_count, &stat_map);
      self.latency_by_quant_distr(&stat_map);
      
      println!("Completed benchmark!");
    }
  }

  fn general_results(&self, reqs: u64, drops: u64, stat_map: &HashMap<ResponseType, Histogram<u64>>) {
    let datetime = chrono::offset::Local::now();
    let header = format!("--- OPEN-LOOP BENCHMARK TEST: {datetime} ---\n");
    
    let setup = format!("SETUP:\n    THREADS: {},\n    CONNECTIONS PER THREAD: {},\n    TARGET RPS: {}\n    BE:LC RATIO: {}\n    LC WRITE:READ RATIO: {}\n\n",
        self.num_threads, self.conns_per_thr, self.target_rps, self.be_lc_ratio, self.lc_wr_ratio);
    let client = format!("CLIENT EFFECTIVENESS:\n    {} REQUESTS SENT / {} SECONDS = {} RPS \n\n",
      reqs, self.runtime_secs, reqs as f64 / self.runtime_secs as f64);
    let throughput = format!("THROUGHPUT: ({} REQUESTS SENT - {} REQUESTS DROPPED) / {} SECONDS = {} TASKS PER SECOND\n\n",
       reqs, drops, self.runtime_secs, (reqs - drops) as f64 / self.runtime_secs as f64);

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
    fs::write("out/benchmark.txt", format!("{header}{setup}{client}{throughput}{stats}{prev}")).unwrap();
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
  conns: Vec<Connection>,
  be_prob: f32,
  lc_wr_prob: f32,
  req_id: u64,
  req_id_mask: u64,
  req_id_shift: u8,
  latencies: HashMap<ResponseType, Vec<u128>>,
  drop_count: u64,
  target_rps: u64,
}

impl ClientThread {
  fn init(port: usize, 
    conns_per_thr: usize, 
    be_prob: f32, 
    lc_wr_prob: f32, 
    req_id_mask: u64, 
    req_id_shift: u8, 
    target_rps: u64) -> Self {
    let mut conns: Vec<Connection> = Vec::new();
    for _ in 0..conns_per_thr {
      conns.push(Connection::new(format!("127.0.0.1:{port}").as_str()).unwrap());
    }

    let mut latencies: HashMap<ResponseType, Vec<u128>> = HashMap::new();
    for t in ResponseType::iterator() {
      latencies.insert(t, Vec::new());
    }
    ClientThread {
        conns,
        latencies,
        be_prob,
        lc_wr_prob,
        req_id: req_id_mask,
        req_id_mask,
        req_id_shift,
        drop_count: 0,
        target_rps
    }
  }

  fn generate_random_request(&mut self) -> (Request, u64) {
    let req_id = self.req_id;
    self.req_id = (((self.req_id >> self.req_id_shift) + 1) << self.req_id_shift) | self.req_id_mask;
    let be_rat: f32 = rand::rng().random();
    let wr_rat: f32 = rand::rng().random();
    if be_rat <= self.be_prob {
      (Request::random(RequestType::BeRead, req_id), req_id)
    } else if wr_rat <= self.lc_wr_prob {
      (Request::random(RequestType::LcWrite, req_id), req_id)
    } else {
      (Request::random(RequestType::LcRead, req_id), req_id)
    }
  }

  fn send_packets(mut self, runtime_secs: f32) -> Result<Self, AspenRsError> {
    let exp = Exp::new(self.target_rps as f64).unwrap();
    let mut rng = rand::rng();
    let n = self.conns.len();
    let start_time = Instant::now();
    let mut next_fire = exp.sample(&mut rng);
  
    loop {
      if start_time.elapsed().as_secs_f32() > runtime_secs {
        break;
      }
      while start_time.elapsed().as_secs_f64() > next_fire {
        // send/enqueue request
        let (req, req_id) = self.generate_random_request();
        
        let conn = &mut self.conns[rand::random_range(0..n)];
        conn.enqueue_new_request(req, req_id)?;

        next_fire += exp.sample(&mut rng);
      }
      
      // progress writes
      for conn in &mut self.conns {
        if !conn.write_queue.is_empty() && 
          OpenProgress::ConnectionReset == conn.progress_writes()? {
          conn.reconnect()?;
        }
      }

      // progress reads
      for conn in &mut self.conns {
        if !conn.read_queue.is_empty() && 
          OpenProgress::ConnectionReset == conn.progress_reads()? {
          conn.reconnect()?;
        }
      }
    }

    for conn in &self.conns {
      self.drop_count += conn.drop_count;
      
      for kind in ResponseType::iterator() {
        let latencies = conn.latencies.get(&kind).unwrap();
        self.latencies.get_mut(&kind).unwrap().extend_from_slice(latencies);
      }
    }

    Ok(self)
  }
}

struct Connection {
  stream: TcpStream,

  in_flight: HashMap<u64, RequestState>,
  write_queue: VecDeque<u64>,
  read_queue: VecDeque<u64>,

  latencies: HashMap<ResponseType, Vec<u128>>,
  drop_count: u64,
}

impl Connection {
  fn new(addr: &str) -> Result<Self, NetworkError> {
    let stream = TcpStream::connect(addr)?;
    stream.set_nonblocking(true)?;
    
    let mut latencies: HashMap<ResponseType, Vec<u128>> = HashMap::new();
    for t in ResponseType::iterator() {
      latencies.insert(t, Vec::new());
    }

    Ok(Connection {
        stream,
        in_flight: HashMap::new(),
        write_queue: VecDeque::new(),
        read_queue: VecDeque::new(),
        latencies,
        drop_count: 0
    })
  }

  fn reconnect(&mut self) -> Result<(), NetworkError> {
      let stream = TcpStream::connect(self.stream.local_addr()?)?;
      stream.set_nonblocking(true)?;
      self.stream = stream;
      self.drop_count += self.in_flight.len() as u64;
      self.in_flight = HashMap::new();
      self.read_queue = VecDeque::new();
      self.write_queue = VecDeque::new();
      Ok(())
  }

  fn enqueue_new_request(&mut self, req: Request, req_id: u64) -> Result<(), AspenRsError> {
    let i = self.in_flight.insert(req_id, RequestState::new(req));
    if let Some(req) = i {
      return Err(AspenRsError::InternalError(format!("req_id {req_id} already exists with {:?}", req)));
    }
    self.write_queue.push_back(req_id);
    Ok(())
  }

  fn progress_writes(&mut self) -> Result<OpenProgress, AspenRsError> {
    while self.write_queue.front().is_some() {
      let req_id = self.write_queue.front().unwrap();
      let req = self.in_flight.get_mut(req_id).unwrap();
      match req {
        RequestState::Writing { req_type, start_time, write_buf, offset } => {
          let req_bytes = write_buf.len();
          match self.stream.write(&write_buf[*offset..req_bytes]) {
            Ok(bytes_written) => {
              let was_started = start_time.is_some();
              if !was_started {
                  *start_time = Some(Instant::now());
              }
              if bytes_written + *offset == req_bytes {
                *req = RequestState::Reading { 
                  res_type: ResponseType::from_request(*req_type), 
                  start_time: (*start_time).unwrap(), 
                  read_buf: Vec::new(), 
                  expected_len: None  
                };
                self.write_queue.pop_front().unwrap();
              } else {
                *offset += bytes_written;
                break;
              }
            },
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == ErrorKind::ConnectionReset => {return Ok(OpenProgress::ConnectionReset)},
            Err(e) => {return Err(AspenRsError::NetworkError(NetworkError::from(e)));}
          }
        },
        RequestState::Reading { .. } => {
          return Err(AspenRsError::InternalError(format!("request {req_id} in write queue with read state")));
        },
      }
    }
    Ok(OpenProgress::MadeProgress)
  }

  fn progress_reads(&mut self) -> Result<OpenProgress, AspenRsError> {
    while self.read_queue.front().is_some() {
      let req_id = self.read_queue.front().unwrap();
      let req = self.in_flight.get_mut(req_id).unwrap();
      match req {
        RequestState::Reading { res_type, start_time, read_buf, expected_len } => {
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
              
              // TODO: Add Drop packet handling
              if check_type && *res_type != packet_type {
                return Err(AspenRsError::ParseError(ParseError::UnexpectedMessageType{ exp_type: *res_type, given_type: packet_type }));
              }
    
              if expected_len.is_none() {
                if read_buf.len() < (1 + LEN_LENGTH) {
                  break;
                }
    
                let len_arr: [u8; 8] = read_buf[1..(1+LEN_LENGTH)].try_into().unwrap();
                *expected_len = Some(usize::from_be_bytes(len_arr));
              }
    
              let total_exp_len = 1 + LEN_LENGTH + expected_len.expect("Should not be None based on previous checks");
    
              if read_buf.len() < total_exp_len {
                break;
              } else if read_buf.len() == total_exp_len {
                let _res = Response::deserialize(read_buf).map_err(AspenRsError::ParseError)?;
                // optional check for response
                let latency = start_time.elapsed().as_micros();
                self.latencies.get_mut(&res_type).unwrap().push(latency);
                self.read_queue.pop_front().unwrap();
                // println!("Response {:?} received from {} in {} µs", _res, self.stream.local_addr().unwrap(), latency);
              } else {
                return Err(AspenRsError::ParseError(ParseError::UnexpectedLength { payload_len: read_buf.len(), exp_len: total_exp_len }));
              }
            },
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == ErrorKind::ConnectionReset => return Ok(OpenProgress::ConnectionReset),
            Err(e) => return Err(AspenRsError::NetworkError(NetworkError::from(e)))
          }
        },
        RequestState::Writing { .. } => {
          return Err(AspenRsError::InternalError(format!("request {req_id} in read queue with write state")));
        },
      }
    }
    Ok(OpenProgress::MadeProgress)
  }
}

#[derive(PartialEq, Eq)]
pub enum OpenProgress {
  MadeProgress,
  ConnectionReset,
}

#[derive(Debug)]
enum RequestState {
  Writing {
      req_type: RequestType,
      start_time: Option<Instant>,
      write_buf: Vec<u8>,
      offset: usize, // start writing at this value
  },
  Reading {
      res_type: ResponseType,
      start_time: Instant,
      read_buf: Vec<u8>,
      expected_len: Option<usize>,
  }
}

impl RequestState {
  fn new(req: Request) -> Self {
    let kind = req.kind();
    let write_buf = req.serialize();
    RequestState::Writing { 
      req_type: kind, 
      start_time: None, 
      write_buf, 
      offset: 0
    }
  }

  fn kind(&self) -> RequestStateType {
    match &self {
        RequestState::Writing { .. } => RequestStateType::Writing,
        RequestState::Reading { .. } => RequestStateType::Reading,
    }
  }
}

#[derive(PartialEq, Eq)]
enum RequestStateType {
  Writing,
  Reading
}