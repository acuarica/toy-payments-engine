use std::{env, error::Error, fs::File, io, process};

use toy_payments_engine::csv::{process_transactions, write_transactions};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() == 2 {
        let file = File::open(&args[1])?;
        let txs = process_transactions(file)?;
        write_transactions(&txs, io::stdout())
    } else {
        eprintln!(
            "Usage: {} <path-to-transactions.csv>",
            env!("CARGO_BIN_NAME")
        );
        process::exit(exitcode::USAGE);
    }
}
