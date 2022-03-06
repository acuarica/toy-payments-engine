//! The `csv` module is used to read/write transactions from/to a
//! CSV buffer, _e.g._, a file or a string.

#![warn(missing_docs)]

use std::{error, io};

use csv::{ReaderBuilder, Trim};
use log::warn;

use crate::{Tx, Txs};

/// Parses and processes incoming transactions from a file.
///
/// # Examples
///
/// ```
/// use toy_payments_engine::csv::*;
///
/// let data = "\
/// type, client, tx, amount
/// deposit, 1, 1, 1.0
/// deposit, 2, 2, 2.0
/// deposit, 1, 3, 2.0
/// withdrawal, 1, 4, 1.5
/// withdrawal, 2, 5, 3.0
/// dispute, 1, 1
/// resolve, 1, 1
/// dispute, 1, 1
/// chargeback, 1, 1
/// ";
///
/// let txs = process_transactions(data.as_bytes()).unwrap();
/// ```
pub fn process_transactions<R: io::Read>(rdr: R) -> Result<Txs, Box<dyn error::Error>> {
    let mut reader = ReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .from_reader(rdr);
    let mut txs = Txs::new();
    let mut lineno = 1;
    for result in reader.deserialize() {
        let tx: Tx = result?;
        if let Err(err) = txs.process_tx(tx) {
            warn!("Warning in line {}: {:?}", lineno, err);
        }
        lineno += 1;
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
/// txs.deposit(1, 1001, dec!(10.05)).unwrap();
/// txs.withdrawal(1, 1002, dec!(1)).unwrap();
///
/// write_transactions(&txs, BufWriter::new(&mut buf)).unwrap();
///
/// assert_eq!(
///     std::str::from_utf8(&buf).unwrap(),
///     "client,available,held,total,locked
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

    use rust_decimal_macros::dec;

    use crate::{Account, Txs};

    use super::{process_transactions, write_transactions};

    #[test]
    fn test_process_transactions_with_errors() {
        let data = "\
type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 7.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0
dispute, 1, 1
dispute, 1, 1
resolve, 1, 1
resolve, 1, 1
dispute, 2, 2
chargeback, 2, 2
";

        let txs = process_transactions(data.as_bytes()).unwrap();
        assert_eq!(
            txs.accounts.get(&1).unwrap(),
            &Account::new(dec!(1.5), dec!(0), false)
        );
        assert_eq!(
            txs.accounts.get(&2).unwrap(),
            &Account::new(dec!(-3), dec!(0), true)
        );
    }

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
