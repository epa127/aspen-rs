use std::{collections::HashMap, fs::File};
use smol::{future::yield_now, lock::RwLock};

use crate::{CAPACITY, YIELD_FREQ};

pub struct Store {
  pub store: RwLock<HashMap<usize, String>>
}

impl Store {
  pub fn new() -> (Self, usize) {
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

  pub async fn lc_read_task(&self, key: usize) -> Option<String> {
    self.store.read().await.get(&key).cloned()
  }

  pub async fn lc_write_task(&self, key: usize, value: String) -> Option<String> {
    self.store.write().await.insert(key, value)
  }

  pub async fn be_task(&self, substring: String) -> usize {
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