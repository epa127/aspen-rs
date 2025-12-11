use std::{sync::mpsc, thread};

use aspen_rust::{client::{closed, open}, server, store::Store};

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
        server::DefaultSmolServer::init(
            1, 
            port, 
            tx, 
            store, 
            1, 
            10000000);
    });

    rx.recv().unwrap();

    println!("Starting main client thread...");
    
    // closed::ClosedBench::new(
    //     5, 
    //     1.0, 
    //     0.0, 
    //     1, 
    //     1).run(port);

    open::OpenBench::new(
        1, 
        1.0,
        0.0, 
        0.0, 
        1,
        1).run(port);
}

