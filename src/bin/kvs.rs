use clap::Parser;
use kvs::{Error, KvStore, Result};
use std::{env::current_dir, process::exit};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about=None)]
enum Args {
    #[command(arg_required_else_help = true)]
    Get { key: String },

    #[command(arg_required_else_help = true)]
    Set { key: String, value: String },

    #[command(arg_required_else_help = true)]
    Rm { key: String },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args {
        Args::Get { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.get(key.clone())? {
                Some(v) => println!("{v}"),
                None => println!("Key not found"),
            }
        }
        Args::Set { key, value } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?;
        }
        Args::Rm { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key) {
                Ok(_) => {}
                Err(Error::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            };
        }
    };
    Ok(())
}
