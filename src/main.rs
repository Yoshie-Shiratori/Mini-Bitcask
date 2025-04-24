mod index;
mod segment;
mod server;
mod storage;

use clap::{Arg, Command};
use server::run_server;

#[tokio::main]
async fn main() {
    let matches = Command::new("Mini-Bitcask")
        .arg(
            Arg::new("address")
                .help("Enter a github username")
                .required(true),
        )
        .get_matches();
    let addr = matches
        .get_one::<String>("address")
        .expect("Address is required");
   
    run_server(addr).await.unwrap();
}
