use std::{sync::mpsc, thread};

use aspen_rust::{client, server::{self, Server}};

fn main() {
    println!("Starting benchmark...");
    let port = 12345;
    let (tx, rx) = mpsc::sync_channel::<()>(1);

    thread::spawn(move || {
        server::DefaultSmolServer::init(port, tx);
    });

    rx.recv().unwrap();

    println!("Starting main client thread...");
    
    client::Client::new(250000, 0.0001, 8, 32).run_benchmark(port);
}

