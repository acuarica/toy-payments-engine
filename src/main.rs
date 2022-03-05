use std::{env, error::Error, process};

use toy_payments_engine::parse_transactions;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() == 2 {
        parse_transactions(&args[1])
    } else {
        eprintln!(
            "Usage: {} <path-to-transactions.csv>",
            env!("CARGO_BIN_NAME")
        );
        process::exit(exitcode::USAGE);
    }
}
