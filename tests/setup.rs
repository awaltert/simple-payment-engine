use std::sync::Arc;

use toy_payment_engine::prelude::{InMemoryAccountRepository, InMemoryTxRepository, PaymentEngine};

pub struct Components {
    pub engine: PaymentEngine<InMemoryAccountRepository, InMemoryTxRepository>,
    pub accounts: Arc<InMemoryAccountRepository>,
    pub transactions: Arc<InMemoryTxRepository>,
}

impl Components {
    pub fn setup() -> Self {
        let accounts = Arc::new(InMemoryAccountRepository::new());
        let transactions = Arc::new(InMemoryTxRepository::new());
        let engine = PaymentEngine::new(Arc::clone(&accounts), Arc::clone(&transactions));

        Self {
            engine,
            transactions,
            accounts,
        }
    }
}
