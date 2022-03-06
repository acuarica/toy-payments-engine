//! The `toy-payments-engine` crate is used to process multiple transactions
//! from a CSV buffer, _e.g._, a file or a string.

#![warn(missing_docs)]

pub mod csv;

use std::collections::{hash_map::Entry, HashMap};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

type Txid = u32;

type Cid = u16;

/// Represents an incoming transaction.
#[derive(Debug, Deserialize)]
pub struct Tx {
    /// The transaction kind of this `tx`.
    #[serde(rename = "type")]
    pub kind: TxKind,
    #[serde(rename = "client")]
    cid: Cid,
    #[serde(rename = "tx")]
    txid: Txid,
    amount: Option<Decimal>,
}

impl Tx {
    /// Creates a new incoming deposit transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use toy_payments_engine::*;
    /// assert_eq!(Tx::deposit(1, 1000, rust_decimal_macros::dec!(1)).kind, TxKind::Deposit);
    /// ```
    pub fn deposit(cid: Cid, txid: Txid, amount: Decimal) -> Self {
        Self {
            kind: TxKind::Deposit,
            cid,
            txid,
            amount: Some(amount),
        }
    }

    /// Creates a new incoming withdrawal transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use toy_payments_engine::*;
    /// assert_eq!(Tx::withdrawal(1, 1000, rust_decimal_macros::dec!(1)).kind, TxKind::Withdrawal);
    /// ```
    pub fn withdrawal(cid: Cid, txid: Txid, amount: Decimal) -> Self {
        Self {
            kind: TxKind::Withdrawal,
            cid,
            txid,
            amount: Some(amount),
        }
    }

    /// Creates a new incoming dispute transaction.
    /// Please note that this type of transaction does not take an amount.
    /// The amount is taken from the corresponding `txid`.
    pub fn dispute(cid: Cid, txid: Txid) -> Self {
        Self {
            kind: TxKind::Dispute,
            cid,
            txid,
            amount: None,
        }
    }
}

/// Represents the state of a given client's account.
#[derive(Debug)]
pub struct Account {
    /// The funds that are available for trading, staking, withdrawal, _etc_.
    pub available: Decimal,
    /// The fund that are held for dispute.
    held: Decimal,
    /// Wheater the account is locked.
    /// An account is locked if a charge back occurs.
    locked: bool,
}

impl Account {
    fn new() -> Self {
        Self {
            available: dec!(0),
            held: dec!(0),
            locked: false,
        }
    }
}

#[derive(Debug, PartialEq)]
/// Represents the kind of errors returned by `Txs::process_tx`.
pub enum Error {
    /// When an overflow or underflow error happens.
    MathError,
    /// When the transaction ID was already processed.
    TxAlreadyExists,
    /// When the transaction ID was not found.
    TxNotFound,
    /// Then transaction is not well formed.
    InvalidTx,
}

/// Represents a collection of incoming transactions to be processed.
#[derive(Debug)]
pub struct Txs {
    txs: HashMap<Txid, Tx>,
    accounts: HashMap<Cid, Account>,
}

impl Txs {
    /// Creates an empty `Txs`.
    ///
    /// The `Txs` is initialized with no transactions and no accounts.
    /// Use the `process_tx` method to append incoming transactions to this `Txs`.
    ///
    /// # Examples
    ///
    /// ```
    /// let txs = toy_payments_engine::Txs::new();
    /// ```
    pub fn new() -> Self {
        Self {
            txs: HashMap::new(),
            accounts: HashMap::new(),
        }
    }

    /// Returns an account if exists, otherwise `None`.
    pub fn get(&self, cid: Cid) -> Option<&Account> {
        self.accounts.get(&cid)
    }

    /// Process a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use toy_payments_engine::*;
    /// use rust_decimal_macros::*;
    ///
    /// let mut txs = Txs::new();
    /// assert_eq!(txs.process_tx(Tx::deposit(1, 1000, dec!(10))).unwrap(), () );
    /// assert_eq!(txs.get(1).unwrap().available, dec!(10) );
    /// ```
    pub fn process_tx(&mut self, tx: Tx) -> Result<(), Error> {
        let account = self.accounts.entry(tx.cid).or_insert(Account::new());

        match (tx.kind, tx.amount) {
            (TxKind::Deposit, Some(amount)) => {
                Txs::process_operation(&mut self.txs, tx, account, amount, Decimal::checked_add)
            }
            (TxKind::Withdrawal, Some(amount)) => {
                Txs::process_operation(&mut self.txs, tx, account, amount, Decimal::checked_sub)
            }
            (TxKind::Dispute, None) => {
                if let Some(operation_tx) = self.txs.get(&tx.txid) {
                    account.available -= operation_tx.amount.unwrap();
                    account.held += operation_tx.amount.unwrap();
                    Ok(())
                } else {
                    Err(Error::TxNotFound)
                }
            }
            (TxKind::Resolve, None) => {
                if let Some(operation_tx) = self.txs.get(&tx.txid) {
                    account.available += operation_tx.amount.unwrap();
                    account.held -= operation_tx.amount.unwrap();
                    Ok(())
                } else {
                    Err(Error::TxAlreadyExists)
                }
            }
            (TxKind::ChargeBack, None) => Ok(()),
            _ => Err(Error::InvalidTx),
        }
    }

    /// Processes an incoming `deposit` transaction.
    pub fn deposit_tx(&mut self, cid: Cid, txid: Txid, amount: Decimal) -> Result<(), Error> {
        self.process_tx(Tx::deposit(cid, txid, amount))
    }

    /// Processes an incoming `withdrawal` transaction.
    pub fn withdrawal_tx(&mut self, cid: Cid, txid: Txid, amount: Decimal) -> Result<(), Error> {
        self.process_tx(Tx::withdrawal(cid, txid, amount))
    }

    /// Processed an incoming `dispute` transaction.
    pub fn dispute(&mut self, cid: Cid, txid: Txid) -> Result<(), Error> {
        self.process_tx(Tx::dispute(cid, txid))
    }

    fn process_operation<F: FnOnce(Decimal, Decimal) -> Option<Decimal>>(
        txs: &mut HashMap<Txid, Tx>,
        tx: Tx,
        account: &mut Account,
        amount: Decimal,
        operation: F,
    ) -> Result<(), Error> {
        if let Some(new_available) = operation(account.available, amount) {
            if let Entry::Vacant(entry) = txs.entry(tx.txid) {
                if let Some(_) = Decimal::checked_add(new_available, account.held) {
                    entry.insert(tx);
                    account.available = new_available;
                    Ok(())
                } else {
                    Err(Error::MathError)
                }
            } else {
                Err(Error::TxAlreadyExists)
            }
        } else {
            Err(Error::MathError)
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Represents the kind of transactions that can be processed.
pub enum TxKind {
    /// A client's deposit into an account.
    Deposit,
    /// A client's withdrawal into an account.
    Withdrawal,
    /// A dispute represents a client's claim that a transaction was erroneous and should be reversed.
    Dispute,
    /// A resolve represents a resolution to a dispute, releasing the associated held funds.
    Resolve,
    /// A chargeback is the final state of a dispute and represents the client reversing a transaction.
    ChargeBack,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{Error, Tx, Txs};

    #[test]
    fn test_deposit() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, dec!(15.005)).unwrap();
        txs.deposit_tx(1, 1002, dec!(24.996)).unwrap();

        txs.deposit_tx(2, 1003, dec!(30)).unwrap();

        txs.deposit_tx(3, 1004, dec!(5)).unwrap();

        txs.deposit_tx(1, 1005, dec!(5.2)).unwrap();

        assert_eq!(txs.get(1).unwrap().available, dec!(45.201));
        assert_eq!(txs.get(2).unwrap().available, dec!(30));
        assert_eq!(txs.get(3).unwrap().available, dec!(5));
    }

    #[test]
    fn test_deposit_overflow() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, Decimal::MAX).unwrap();

        assert_eq!(
            txs.deposit_tx(1, 1002, dec!(1)).unwrap_err(),
            Error::MathError
        );
    }

    #[test]
    fn test_withdrawal() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, dec!(15.005)).unwrap();
        txs.deposit_tx(1, 1002, dec!(24.996)).unwrap();

        txs.deposit_tx(2, 1003, dec!(10)).unwrap();

        assert_eq!(txs.get(1).unwrap().available, dec!(40.001));
        assert_eq!(txs.get(2).unwrap().available, dec!(10));

        txs.withdrawal_tx(1, 1004, dec!(10.002)).unwrap();
        txs.withdrawal_tx(2, 1005, dec!(10)).unwrap();

        assert_eq!(txs.get(1).unwrap().available, dec!(29.999));
        assert_eq!(txs.get(2).unwrap().available, dec!(0));
    }

    #[test]
    fn test_withdrawal_underflow() {
        let mut txs = Txs::new();
        txs.withdrawal_tx(1, 1001, Decimal::MAX).unwrap();

        assert_eq!(
            txs.withdrawal_tx(1, 1002, dec!(1)).unwrap_err(),
            Error::MathError
        );
    }

    #[test]
    fn test_deposit_same_tx() {
        let mut txs = Txs::new();
        txs.process_tx(Tx::deposit(1, 1001, dec!(10))).unwrap();

        assert_eq!(
            txs.process_tx(Tx::deposit(1, 1001, dec!(10))).unwrap_err(),
            Error::TxAlreadyExists
        );
        assert_eq!(
            txs.process_tx(Tx::deposit(2, 1001, dec!(10))).unwrap_err(),
            Error::TxAlreadyExists
        );
    }

    #[test]
    fn test_total_overflow_when_deposit() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, Decimal::MAX).unwrap();
        txs.dispute(1, 1001).unwrap();

        assert_eq!(
            txs.deposit_tx(1, 1002, dec!(1)).unwrap_err(),
            Error::MathError
        );
    }
}
