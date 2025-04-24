mod server;
mod index;
mod segment;
mod storage;

use server::run_server;
use console_subscriber;
#[tokio::main]
async fn main() {
    //let path = Path::new("data.db");
    //let mut storage = Storage::new(path).unwrap();
    //
    //storage.begin_transaction().unwrap();
    //storage.set("user1", "data1").unwrap();
    //storage.set("user2", "data2").unwrap();
    //storage.commit().unwrap();
    //
    //println!("{:?}", storage.get("user1"));
    //
    //storage.delete("user2").unwrap();
    //
    console_subscriber::init();
    let addr = "127.0.0.1:8080";
    run_server(addr).await.unwrap();
}
