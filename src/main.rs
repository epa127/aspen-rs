use std::{sync::mpsc, thread};

use aspen_rust::{client, server, store::Store};

fn main() {
    println!("Starting benchmark...");
    let port = 12345;
    let (tx, rx) = mpsc::sync_channel::<()>(1);

    println!("Building database...");
    let (store, store_len) = Store::new();
    println!("Successfully created database with {} keys.", store_len);

    let num_threads = num_cpus::get();
    let client_threads: usize = 3;
    let server_threads = num_threads - client_threads;
    thread::spawn(move || {
        server::DefaultSmolServer::init(server_threads, port, tx, store);
    });

    rx.recv().unwrap();

    println!("Starting main client thread...");
    
    client::Client::new(2500, 0.1, 0.1, client_threads, 256).run(port);
}

