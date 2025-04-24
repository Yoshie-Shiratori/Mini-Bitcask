mod index;
mod segment;
mod storage;

use crate::storage::Storage;
use std::path::Path;

fn main() {
    let path = Path::new("data.db");
    let mut storage = Storage::new(path).unwrap();

    storage.begin_transaction().unwrap();
    storage.set("user1", "data1").unwrap();
    storage.set("user2", "data2").unwrap();
    storage.commit().unwrap();

    println!("{:?}", storage.get("user1")); 

    storage.delete("user2").unwrap();
}
