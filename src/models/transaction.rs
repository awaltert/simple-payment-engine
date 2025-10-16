use serde::{Deserialize, Serialize};

use crate::models::NonNegativeDecimal;
use crate::models::client::ClientId;

/// This is the main type used in the stream processed by the engine.
/// Its enum variants are specialized to their use cases.
#[derive(Debug)]
pub enum TxRecord {
    Deposit(Deposit),
    Withdrawal(Withdrawal),
    Dispute(Dispute),
    Resolve(Resolve),
    Chargeback(Chargeback),
}

#[derive(Debug, Clone, Copy)]
pub struct Deposit {
    pub client_id: ClientId,
    pub tx_id: TransactionId,
    pub amount: NonNegativeDecimal,
}

#[derive(Debug, Clone, Copy)]
pub struct Withdrawal {
    pub client_id: ClientId,
    pub tx_id: TransactionId,
    pub amount: NonNegativeDecimal,
}

#[derive(Debug, Clone, Copy)]
pub struct Dispute {
    pub client_id: ClientId,
    pub tx_id: TransactionId,
}

#[derive(Debug, Clone, Copy)]
pub struct Resolve {
    pub client_id: ClientId,
    pub tx_id: TransactionId,
}

#[derive(Debug, Clone, Copy)]
pub struct Chargeback {
    pub client_id: ClientId,
    pub tx_id: TransactionId,
}

impl From<Deposit> for TxRecord {
    fn from(deposit: Deposit) -> Self {
        Self::Deposit(deposit)
    }
}

impl From<Withdrawal> for TxRecord {
    fn from(withdrawal: Withdrawal) -> Self {
        Self::Withdrawal(withdrawal)
    }
}

impl From<Dispute> for TxRecord {
    fn from(dispute: Dispute) -> Self {
        Self::Dispute(dispute)
    }
}

impl From<Resolve> for TxRecord {
    fn from(resolve: Resolve) -> Self {
        Self::Resolve(resolve)
    }
}

impl From<Chargeback> for TxRecord {
    fn from(chargeback: Chargeback) -> Self {
        Self::Chargeback(chargeback)
    }
}

#[derive(Clone, Copy)]
pub struct Transaction {
    pub tx_type: TransactionType,
    pub client_id: ClientId,
    pub id: TransactionId,
    pub amount: NonNegativeDecimal,
    pub status: TransactionStatus,
}

impl Transaction {
    pub fn from_deposit(deposit: Deposit, status: TransactionStatus) -> Self {
        Self {
            id: deposit.tx_id,
            client_id: deposit.client_id,
            tx_type: TransactionType::Deposit,
            amount: deposit.amount,
            status,
        }
    }

    pub fn from_withdrawal(withdrawal: Withdrawal, status: TransactionStatus) -> Self {
        Self {
            id: withdrawal.tx_id,
            client_id: withdrawal.client_id,
            tx_type: TransactionType::Withdrawal,
            amount: withdrawal.amount,
            status,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Processed,
    Failed,
    Disputed,
    Resolved,
    Chargedback,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TransactionId(u32);

impl TransactionId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

/// This type is used in the TransactionRepository and only offers the necessary variants for persisting Deposits and Withdrawals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
}
