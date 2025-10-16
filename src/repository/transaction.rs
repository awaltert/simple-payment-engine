use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::models::transaction::{Transaction, TransactionId, TransactionStatus};

/// This trait is strictly not necessary. I introduced it to showcase a more realistic scenario, in which the engine might be able to use different repository implementations.
/// Due to this, result types are part of the signature, to indicate potential IO.
#[async_trait]
pub trait TransactionRepository: Send + Sync {
    async fn get(&self, tx_id: TransactionId) -> Option<Transaction>;
    async fn insert(&self, tx: Transaction) -> Result<()>;
    async fn update_status(&self, tx_id: TransactionId, status: TransactionStatus) -> Result<()>;
}

/// I decided to use an RwLock, mainly because its usage is recommended if there is inner IO, e.g. to call a database.
/// Another factor is the expected usage pattern. I wanted to showcase a scenario, in which we need a type that is Send+Sync,
/// demonstrating the common Arc - Inner pattern in conjunction with a Mutex.
#[derive(Default, Clone)]
pub struct InMemoryTxRepository {
    inner: Arc<RwLock<Inner>>,
}

impl InMemoryTxRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TransactionRepository for InMemoryTxRepository {
    async fn get(&self, tx_id: TransactionId) -> Option<Transaction> {
        let guard = self.inner.read().await;

        guard.get(tx_id)
    }

    async fn insert(&self, tx: Transaction) -> Result<()> {
        let mut guard = self.inner.write().await;

        guard.insert(tx)
    }

    async fn update_status(&self, tx_id: TransactionId, status: TransactionStatus) -> Result<()> {
        let mut guard = self.inner.write().await;

        guard.update_status(tx_id, status)
    }
}

#[derive(Default)]
struct Inner {
    txs: HashMap<TransactionId, Transaction>,
}

impl Inner {
    fn get(&self, tx_id: TransactionId) -> Option<Transaction> {
        self.txs.get(&tx_id).copied()
    }

    fn insert(&mut self, tx: Transaction) -> Result<()> {
        if self.txs.contains_key(&tx.id) {
            bail!(
                "Failed to insert transaction with id: {:?}. A transaction with the same key has already been persisted",
                tx.id
            );
        }

        self.txs.insert(tx.id, tx);

        Ok(())
    }

    fn update_status(&mut self, tx_id: TransactionId, status: TransactionStatus) -> Result<()> {
        self.txs
            .get_mut(&tx_id)
            .map(|tx| tx.status = status)
            .with_context(|| {
                format!("Failed to resolve transaction: {tx_id:?} and status: {status:?}")
            })
    }
}
