//! The `csv` module is used to read/write transactions from/to a
//! CSV buffer, _e.g._, a file or a string.

#![warn(missing_docs)]

use std::{error, io, path::Path};

use csv::{ReaderBuilder, Trim};

use crate::{Tx, Txs};

/// Parses and processes incoming transactions from a file.
pub fn parse_transactions<P: AsRef<Path>>(path: P) -> Result<Txs, Box<dyn error::Error>> {
    let mut reader = ReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .from_path(path)?;
    let mut txs = Txs::new();
    for result in reader.deserialize() {
        let tx: Tx = result?;
        txs.process_tx(tx).unwrap();
    }

    Ok(txs)
}

/// Write transactions `txs` to a `Write`r `wtr`.
/// These transactions are written in CSV format.
/// The first row contains a header row to indicate column names.
/// Note that there is no particular order when writing the client's accounts.
///
/// # Examples
///
/// ```
/// use std::io::*;
/// use toy_payments_engine::*;
/// use toy_payments_engine::csv::*;
/// use rust_decimal_macros::dec;
///
/// let mut txs = Txs::new();
/// let mut buf = vec![];
///
/// txs.deposit_tx(1, 1001, dec!(10.05));
/// txs.withdrawal_tx(1, 1002, dec!(1));
/// txs.deposit_tx(2, 1003, dec!(5));
/// write_transactions(&txs, BufWriter::new(&mut buf)).unwrap();
///
/// assert_eq!(
///     std::str::from_utf8(&buf).unwrap(),
///     "client,available,held,total,locked
/// 2,5,0,5,false
/// 1,9.05,0,9.05,false
/// "
/// );
/// ```
pub fn write_transactions<W: io::Write>(txs: &Txs, wtr: W) -> Result<(), Box<dyn error::Error>> {
    let mut writer = csv::Writer::from_writer(wtr);

    writer.write_record(&["client", "available", "held", "total", "locked"])?;

    for (cid, account) in &txs.accounts {
        let total = account.available + account.held;
        writer.write_record(&[
            cid.to_string(),
            account.available.to_string(),
            account.held.to_string(),
            total.to_string(),
            account.locked.to_string(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::io::BufWriter;

    use crate::Txs;

    use super::write_transactions;

    #[test]
    fn test_write_empty_transactions() {
        let txs = Txs::new();
        let mut buf = vec![];

        write_transactions(&txs, BufWriter::new(&mut buf)).unwrap();

        assert_eq!(
            std::str::from_utf8(&buf).unwrap(),
            "client,available,held,total,locked\n"
        );
    }
}
