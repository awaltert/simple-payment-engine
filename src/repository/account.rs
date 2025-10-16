use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::models::account::Account;
use crate::models::client::ClientId;

/// This trait is strictly not necessary. I introduced it to showcase a more realistic scenario, in which the engine might be able to use different repository implementations.
/// Due to this, result types are part of the signature, to indicate potential IO.
#[async_trait]
pub trait AccountRepository {
    async fn get(&self, client_id: ClientId) -> Result<Option<Account>>;

    async fn get_or_new(&self, client_id: ClientId) -> Result<Account>;
    async fn upsert(&self, account: Account) -> Result<()>;

    async fn balances(&self) -> Vec<Account>;
}

/// I decided to use an RwLock, mainly because its usage is recommended if there is inner IO, e.g. to call a database.
/// Another factor is the expected usage pattern. I wanted to showcase a scenario, in which we need a type that is Send+Sync,
/// demonstrating the common Arc - Inner pattern in conjunction with a Mutex.
#[derive(Default, Clone)]
pub struct InMemoryAccountRepository {
    inner: Arc<RwLock<Inner>>,
}

impl InMemoryAccountRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl AccountRepository for InMemoryAccountRepository {
    async fn get(&self, client_id: ClientId) -> Result<Option<Account>> {
        let guard = self.inner.read().await;

        let acc = guard.get(client_id);

        Ok(acc)
    }

    async fn get_or_new(&self, client_id: ClientId) -> Result<Account> {
        let guard = self.inner.read().await;

        let acc = guard
            .get(client_id)
            .unwrap_or_else(|| Account::new(client_id));

        Ok(acc)
    }

    async fn upsert(&self, account: Account) -> Result<()> {
        let mut guard = self.inner.write().await;

        guard.upsert(account)
    }

    async fn balances<'a>(&'a self) -> Vec<Account> {
        let guard = self.inner.read().await;

        guard.balances()
    }
}

#[derive(Default)]
struct Inner {
    accounts: HashMap<ClientId, Account>,
}

impl Inner {
    fn get(&self, client_id: ClientId) -> Option<Account> {
        self.accounts.get(&client_id).copied()
    }

    fn upsert(&mut self, account: Account) -> Result<()> {
        self.accounts
            .entry(account.client_id)
            .and_modify(|occupied| {
                occupied.available = account.available;
                occupied.held = account.held;
                occupied.total = account.total;
                occupied.is_locked = account.is_locked
            })
            .or_insert(account);

        Ok(())
    }

    fn balances(&self) -> Vec<Account> {
        self.accounts.values().copied().collect()
    }
}
