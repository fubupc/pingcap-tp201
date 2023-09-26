use clap::Parser;
use std::process::exit;

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

fn main() {
    let args = Args::parse();
    match args {
        Args::Get { .. } => {
            eprintln!("unimplemented!");
        }
        Args::Set { .. } => {
            eprintln!("unimplemented!");
        }
        Args::Rm { .. } => {
            eprintln!("unimplemented!");
        }
    };
    exit(1);
}
