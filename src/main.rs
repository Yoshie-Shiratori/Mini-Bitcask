mod segment;
mod index;
mod storage;

use std::path::Path;
use crate::storage::Storage;

fn main() {
    let path = Path::new("mydata.db");
    
    let mut storage = Storage::new(path).expect("Failed to create storage");

    let key = "greeting".to_string();
    let value = b"hello world".to_vec();

    // Put key-value into storage
    storage.put(key.clone(), value.clone()).expect("Put failed");

    // Get value back
    match storage.get(&key).expect("Get failed") {
        Some(retrieved) => {
            println!("Got value: {}", String::from_utf8_lossy(&retrieved));
        }
        None => {
            println!("Key not found");
        }
    }
}

