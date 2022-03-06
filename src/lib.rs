//! The `toy-payments-engine` crate is used to process multiple transactions
//! from a CSV buffer, _e.g._, a file or a string.

#![warn(missing_docs)]

pub mod csv;

use std::collections::{hash_map::Entry, HashMap};

use rust_decimal::Decimal;
use serde::Deserialize;

type Txid = u32;

type Cid = u16;

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
    #[serde(skip_deserializing)]
    disputed: bool,
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
            disputed: false,
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
            disputed: false,
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
            disputed: false,
        }
    }

    /// Creates a new incoming resolve transaction.
    /// Please note that this type of transaction does not take an amount.
    /// The amount is taken from the corresponding `txid`.
    pub fn resolve(cid: Cid, txid: Txid) -> Self {
        Self {
            kind: TxKind::Resolve,
            cid,
            txid,
            amount: None,
            disputed: false,
        }
    }

    /// Creates a new incoming chargeback transaction.
    /// Please note that this type of transaction does not take an amount.
    /// The amount is taken from the corresponding `txid`.
    pub fn charge_back(cid: Cid, txid: Txid) -> Self {
        Self {
            kind: TxKind::ChargeBack,
            cid,
            txid,
            amount: None,
            disputed: false,
        }
    }
}

/// Represents the state of a given client's account.
#[derive(Debug, PartialEq, Default)]
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
    /// Creates a new account.
    pub fn new(available: Decimal, held: Decimal, locked: bool) -> Self {
        Self {
            available,
            held,
            locked,
        }
    }
}

#[derive(Debug, PartialEq)]
/// Represents the kind of errors returned by `Txs::process_tx`.
pub enum Error {
    /// Occurs when an overflow or underflow error happens.
    MathError,
    /// Occurs when the transaction ID was already processed.
    TxAlreadyExists,
    /// Occurs when the transaction ID was not found.
    TxNotFound,
    /// Occurs when two TXs have a different Client ID.
    CidMismatch,
    /// Occurs when a TX is being disputed a second time.
    TxAlreadyDisputed,
    /// Occurs when a TX is not being disputed.
    TxNotDisputed,
    /// When transaction is not well formed.
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

    /// Processes an incoming deposit transaction.
    pub fn deposit_tx(&mut self, cid: Cid, txid: Txid, amount: Decimal) -> Result<(), Error> {
        self.process_tx(Tx::deposit(cid, txid, amount))
    }

    /// Processes an incoming withdrawal transaction.
    pub fn withdrawal_tx(&mut self, cid: Cid, txid: Txid, amount: Decimal) -> Result<(), Error> {
        self.process_tx(Tx::withdrawal(cid, txid, amount))
    }

    /// Processes an incoming dispute transaction.
    pub fn dispute(&mut self, cid: Cid, txid: Txid) -> Result<(), Error> {
        self.process_tx(Tx::dispute(cid, txid))
    }

    /// Processes an incoming resolve transaction.
    pub fn resolve(&mut self, cid: Cid, txid: Txid) -> Result<(), Error> {
        self.process_tx(Tx::resolve(cid, txid))
    }

    /// Processes an incoming charge back transaction.
    pub fn charge_back(&mut self, cid: Cid, txid: Txid) -> Result<(), Error> {
        self.process_tx(Tx::charge_back(cid, txid))
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
        let account = self.accounts.entry(tx.cid).or_default();
        match (tx.kind, tx.amount) {
            (TxKind::Deposit, Some(amount)) => {
                Txs::process_operation(&mut self.txs, tx, account, amount, Decimal::checked_add)
            }
            (TxKind::Withdrawal, Some(amount)) => {
                Txs::process_operation(&mut self.txs, tx, account, amount, Decimal::checked_sub)
            }
            (TxKind::Dispute, None) => self.with_tx(tx, |ref_tx, account| {
                if !ref_tx.disputed {
                    account.available -= ref_tx.amount.unwrap();
                    account.held += ref_tx.amount.unwrap();
                    ref_tx.disputed = true;
                    Ok(())
                } else {
                    Err(Error::TxAlreadyDisputed)
                }
            }),
            (TxKind::Resolve, None) => self.with_tx(tx, |ref_tx, account| {
                if ref_tx.disputed {
                    account.available += ref_tx.amount.unwrap();
                    account.held -= ref_tx.amount.unwrap();
                    ref_tx.disputed = false;
                    Ok(())
                } else {
                    Err(Error::TxNotDisputed)
                }
            }),
            (TxKind::ChargeBack, None) => self.with_tx(tx, |ref_tx, account| {
                if ref_tx.disputed {
                    account.held -= ref_tx.amount.unwrap();
                    account.locked = true;
                    ref_tx.disputed = false;
                    Ok(())
                } else {
                    Err(Error::TxNotDisputed)
                }
            }),
            _ => Err(Error::InvalidTx),
        }
    }

    fn with_tx<F: FnOnce(&mut Tx, &mut Account) -> Result<(), Error>>(
        &mut self,
        tx: Tx,
        op: F,
    ) -> Result<(), Error> {
        let account = self.accounts.entry(tx.cid).or_default();
        self.txs
            .get_mut(&tx.txid)
            .ok_or_else(|| Error::TxNotFound)
            .and_then(|ref_tx| {
                if ref_tx.cid == tx.cid {
                    op(ref_tx, account)
                } else {
                    Err(Error::CidMismatch)
                }
            })
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

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{Account, Error, Tx, Txs};

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
    fn test_dispute() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, dec!(20)).unwrap();
        txs.deposit_tx(1, 1002, dec!(10)).unwrap();
        txs.dispute(1, 1001).unwrap();

        assert_eq!(
            txs.get(1).unwrap(),
            &Account::new(dec!(10), dec!(20), false)
        );
    }

    #[test]
    fn test_tx_not_found() {
        let mut txs = Txs::new();
        assert_eq!(txs.dispute(1, 1001).unwrap_err(), Error::TxNotFound);
        assert_eq!(txs.resolve(1, 1001).unwrap_err(), Error::TxNotFound);
        assert_eq!(txs.charge_back(1, 1001).unwrap_err(), Error::TxNotFound);
    }

    #[test]
    fn test_cid_mismatch() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, dec!(10)).unwrap();

        assert_eq!(txs.dispute(2, 1001).unwrap_err(), Error::CidMismatch);
        assert_eq!(txs.resolve(2, 1001).unwrap_err(), Error::CidMismatch);
        assert_eq!(txs.charge_back(2, 1001).unwrap_err(), Error::CidMismatch);
    }

    #[test]
    fn test_double_dispute() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, dec!(20)).unwrap();
        txs.deposit_tx(1, 1002, dec!(10)).unwrap();
        txs.dispute(1, 1001).unwrap();
        assert_eq!(txs.dispute(1, 1001).unwrap_err(), Error::TxAlreadyDisputed);

        assert_eq!(
            txs.get(1).unwrap(),
            &Account::new(dec!(10), dec!(20), false)
        );
    }

    #[test]
    fn test_tx_not_disputed() {
        let mut txs = Txs::new();
        txs.deposit_tx(1, 1001, dec!(20)).unwrap();
        assert_eq!(txs.resolve(1, 1001).unwrap_err(), Error::TxNotDisputed);
        assert_eq!(txs.charge_back(1, 1001).unwrap_err(), Error::TxNotDisputed);
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
