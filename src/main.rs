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
    
    client::Client::new(25, 0.0001, 0.1, 4, 16).run(port);
}

