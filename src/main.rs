use std::{env, error::Error, io, process};

use toy_payments_engine::{parse_transactions, write_transactions};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() == 2 {
        let txs = parse_transactions(&args[1])?;
        write_transactions(&txs, io::stdout())
    } else {
        eprintln!(
            "Usage: {} <path-to-transactions.csv>",
            env!("CARGO_BIN_NAME")
        );
        process::exit(exitcode::USAGE);
    }
}
