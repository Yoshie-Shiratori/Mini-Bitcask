use crate::storage::Storage;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Start the TCP server
pub async fn run_server(addr: &str) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr).await?; // Create a listener on the specified address
    let storage = Arc::new(Mutex::new(
        Storage::new(std::path::Path::new("./db")).unwrap(),
    )); // Data storage

    println!("Server listening on {}", addr);

    // Connection waiting loop
    loop {
        let (socket, _) = listener.accept().await?; // Wait for new connections
        let storage_clone = Arc::clone(&storage);

        // For each client, spawn an asynchronous task
        tokio::spawn(async move {
            handle_client(socket, storage_clone).await;
        });
    }
}

/// Handle the client connection
async fn handle_client(mut stream: TcpStream, storage: Arc<Mutex<Storage>>) {
    let mut buffer = [0; 1024]; // Buffer for reading data

    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break, // If connection is closed, break the loop
            Ok(n) => {
                let request = String::from_utf8_lossy(&buffer[..n]); // Convert bytes to string
                println!("Received request: {}", request);

                // Process the request
                let response = process_request(&request, &storage);

                // Send the response back to the client
                if let Err(e) = stream.write_all(response.as_bytes()).await {
                    eprintln!("Failed to send response: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error reading from stream: {}", e);
                break;
            }
        }
    }

    println!("Client disconnected");
}

/// Process the request based on the command
fn process_request(request: &str, storage: &Arc<Mutex<Storage>>) -> String {
    let parts: Vec<&str> = request.trim().split_whitespace().collect(); // Split the input by spaces

    match parts.as_slice() {
        ["SET", key, value] => handle_set_command(key, value, storage),
        ["GET", key] => handle_get_command(key, storage),
        ["DELETE", key] => handle_delete_command(key, storage),
        ["BEGIN"] => handle_transaction_command("BEGIN", storage),
        ["COMMIT"] => handle_transaction_command("COMMIT", storage),
        ["ROLLBACK"] => handle_transaction_command("ROLLBACK", storage),
        _ => "ERROR: Invalid command format\n".to_string(),
    }
}

/// Handle the SET command
fn handle_set_command(key: &str, value: &str, storage: &Arc<Mutex<Storage>>) -> String {
    match storage.lock().unwrap().set(key, value) {
        Ok(offset) => format!("SET OK, offset: {}\n", offset),
        Err(e) => format!("ERROR: {}\n", e),
    }
}

/// Handle the GET command
fn handle_get_command(key: &str, storage: &Arc<Mutex<Storage>>) -> String {
    match storage.lock().unwrap().get(key) {
        Ok(Some(value)) => format!("VALUE: {}\n", value),
        Ok(None) => "NOT FOUND\n".to_string(),
        Err(e) => format!("ERROR: {}\n", e),
    }
}

/// Handle the DELETE command
fn handle_delete_command(key: &str, storage: &Arc<Mutex<Storage>>) -> String {
    match storage.lock().unwrap().delete(key) {
        Ok(_) => "DELETE OK\n".to_string(),
        Err(e) => format!("ERROR: {}\n", e),
    }
}

/// Handle transaction commands (BEGIN, COMMIT, ROLLBACK)
fn handle_transaction_command(command: &str, storage: &Arc<Mutex<Storage>>) -> String {
    let mut storage = storage.lock().unwrap();
    match command {
        "BEGIN" => match storage.begin_transaction() {
            Ok(_) => "BEGIN TRANSACTION OK\n".to_string(),
            Err(_) => "ERROR\n".to_string(),
        },
        "COMMIT" => match storage.commit() {
            Ok(_) => "COMMIT OK\n".to_string(),
            Err(_) => "ERROR\n".to_string(),
        },
        "ROLLBACK" => match storage.rollback() {
            Ok(_) => "ROLLBACK OK\n".to_string(),
            Err(_) => "ERROR\n".to_string(),
        },
        _ => "ERROR: Invalid transaction command\n".to_string(),
    }
}
