use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use futures::stream::{FusedStream, StreamExt};
use tokio::pin;
use tracing::error;

use crate::models::account::Direction;
use crate::models::transaction::{
    Chargeback, Deposit, Dispute, Resolve, Transaction, TransactionStatus, TransactionType,
    TxRecord, Withdrawal,
};
use crate::repository::account::AccountRepository;
use crate::repository::transaction::TransactionRepository;

/// We use static dispatch for the engine, as dyn dispatch is not necessary for this use case.
/// Dyn dispatch would introduce some complexity in the correspondig respository types, as well.
pub struct PaymentEngine<AR, TR> {
    accounts: Arc<AR>,
    transactions: Arc<TR>,
}

impl<AR, TR> PaymentEngine<AR, TR>
where
    AR: AccountRepository,
    TR: TransactionRepository,
{
    pub fn new(accounts: Arc<AR>, transactions: Arc<TR>) -> Self {
        Self {
            accounts,
            transactions,
        }
    }

    /// This is the main entry point for processing the entities on the stream
    pub async fn process<S>(&self, stream: S)
    where
        S: FusedStream<Item = TxRecord>,
    {
        pin!(stream);

        while let Some(tx) = stream.next().await {
            // The final result marks the root element, which makes it an ideal candidate for being reported in a telemetry system.
            // Because this experiment does not have proper telemtry, I decided to drop it (see the inspect_err below)
            let _res: Result<()> = match tx {
                // Main dispatcher & extension point:
                // 1. if new variants might come up
                // 2. but also to change the runtime behavior, e.g by spawning the work as dedicated tasks
                TxRecord::Deposit(deposit) => {
                    let existing_tx = self.transactions.get(deposit.tx_id).await;

                    match prevent_replay_attack(existing_tx.as_ref()) {
                        Ok(()) => self.handle_deposit(deposit).await,
                        Err(err) => Err(err),
                    }
                }

                TxRecord::Withdrawal(withdrawal) => {
                    let existing_tx = self.transactions.get(withdrawal.tx_id).await;

                    match prevent_replay_attack(existing_tx.as_ref()) {
                        Ok(()) => self.handle_withdrawal(withdrawal).await,
                        Err(err) => Err(err),
                    }
                }

                TxRecord::Dispute(dispute) => {
                    let referenced_tx = self.transactions.get(dispute.tx_id).await;

                    match tx_exists_and_has_been_processed(referenced_tx.as_ref()) {
                        Ok(tx) => {
                            // Remark: the into_inner is a shortcut because lack of time.
                            // It would be better to extend the NonNegativeDecimal, allowing the necessary operations
                            let amount = tx.amount.into_inner();
                            // Direction is a workaround. Please see the comment on the type definition
                            let direction = if tx.tx_type == TransactionType::Withdrawal {
                                Direction::Increase(amount)
                            } else {
                                Direction::Decrease(amount)
                            };

                            self.handle_dispute(dispute, direction).await
                        }

                        Err(err) => Err(err),
                    }
                }

                TxRecord::Resolve(resolve) => {
                    let referenced_tx = self.transactions.get(resolve.tx_id).await;

                    match tx_exists_and_has_been_disputed(referenced_tx.as_ref()) {
                        Ok(tx) => {
                            let amount = tx.amount.into_inner();

                            let direction = if tx.tx_type == TransactionType::Withdrawal {
                                Direction::Increase(amount)
                            } else {
                                Direction::Decrease(amount)
                            };

                            self.handle_resolve(resolve, direction).await
                        }

                        Err(err) => Err(err),
                    }
                }

                TxRecord::Chargeback(cb) => {
                    let referenced_tx = self.transactions.get(cb.tx_id).await;

                    match tx_exists_and_has_been_resolved(referenced_tx.as_ref()) {
                        Ok(tx) => {
                            let amount = tx.amount.into_inner();

                            let direction = if tx.tx_type == TransactionType::Withdrawal {
                                Direction::Increase(amount)
                            } else {
                                Direction::Decrease(amount)
                            };

                            self.handle_chargeback(cb, direction).await
                        }

                        Err(err) => Err(err),
                    }
                }
            }
            .inspect_err(|err| {
                error!("Failed to process TxRecord: {err:?}");
            });
        }
    }

    async fn handle_deposit(&self, deposit: Deposit) -> Result<()> {
        let client_id = deposit.client_id;
        let tx_id = deposit.tx_id;
        // This future serves a very important responsibility:
        // It defines a scope of execution and here also to have some kind of "transactional" context.
        // I find this pattern usefull, because I can post process the result, regardless if we left it early (due to an error and the ? operator) or if the futures succeeded
        let tx = async move {
            let mut new_acc = self.accounts.get_or_new(client_id).await.with_context(|| {
                format!("Failed to get account from AccountRepo for client_id: {client_id:?}")
            })?;

            new_acc.deposit(deposit.amount.into_inner()).with_context(|| format!("Failed to perform the deposit for a client_id: {client_id:?} and tx_id: {tx_id:?}"))?;

            self.accounts.upsert(new_acc).await.with_context(|| {
                format!("Failed to upsert account in AccountRepo: {new_acc:?}")
            })
        }
        .await
        .map(|()| Transaction::from_deposit(deposit, TransactionStatus::Processed))
        .unwrap_or_else(|_err| Transaction::from_deposit(deposit, TransactionStatus::Failed));

        self.transactions.insert(tx).await.with_context(|| {
                format!(
                    "Failed to upsert transaction in TransactionRepo for client_id: {client_id:?} and tx_id: {tx_id:?}",
                )
            })
    }

    async fn handle_withdrawal(&self, withdrawal: Withdrawal) -> Result<()> {
        let client_id = withdrawal.client_id;
        let tx_id = withdrawal.tx_id;

        let tx = async move {
            let acc = self.accounts.get(client_id).await.with_context(|| {
                format!("Failed to get account from AccountRepo for client_id: {client_id:?}")
            })?;

            let mut acc = acc.with_context(|| format!("Failed to withdraw from an non-existing account. Client {client_id:?} and tx_id: {tx_id:?}"))?;

            acc.try_withdrawal(withdrawal.amount.into_inner()).with_context(|| format!("Failed to perform the withdrawal for a client)_id: {client_id:?} and tx_id: {tx_id:?}"))?;

            self.accounts.upsert(acc).await.with_context(|| {
                format!("Failed to upsert account in AccountRepo: {acc:?}")
            })
        }
        .await
        .map(|()| Transaction::from_withdrawal(withdrawal, TransactionStatus::Processed))
        .unwrap_or_else(|_err| Transaction::from_withdrawal(withdrawal, TransactionStatus::Failed));

        self.transactions.insert(tx).await.with_context(|| {
                format!(
                    "Failed to upsert transaction in TransactionRepo for client_id: {client_id:?} and tx_id: {tx_id:?}",
                )
            })
    }

    async fn handle_dispute(&self, dispute: Dispute, direction: Direction) -> Result<()> {
        let client_id = dispute.client_id;
        let tx_id = dispute.tx_id;

        let res = async move {
            let acc = self.accounts.get(client_id).await;

            let Ok(Some(mut acc)) = acc else {
                return Err(anyhow!("Failed to get account from AccountRepo for client_id: {client_id:?}"));
            };

            acc.dispute(direction).with_context(|| format!("Failed to perform the dispute for a client_id: {client_id:?} and tx_id: {tx_id:?}"))?;

            self.accounts.upsert(acc).await.with_context(|| {
                format!("Failed to upsert account in AccountRepo: {acc:?}")
            })
        }
        .await;

        if res.is_ok() {
            return self.transactions.update_status(tx_id, TransactionStatus::Disputed).await.with_context(|| {
                format!(
                    "Failed to update transaction status: disputed for client_id: {client_id:?} and tx_id: {tx_id:?}",
                )
            });
        }

        res
    }

    async fn handle_resolve(&self, resolve: Resolve, direction: Direction) -> Result<()> {
        let client_id = resolve.client_id;
        let tx_id = resolve.tx_id;

        let res = async move {
            let acc = self.accounts.get(client_id).await;

            let Ok(Some(mut acc)) = acc else {
                return Err(anyhow!("Failed to get account from AccountRepo for client_id: {client_id:?}"));
            };

            acc.resolve(direction).with_context(|| format!("Failed to perform the resolve for a client_id: {client_id:?} and tx_id: {tx_id:?}"))?;

            self.accounts.upsert(acc).await.with_context(|| {
                format!("Failed to upsert account in AccountRepo: {acc:?}")
            })
        }
        .await;

        if res.is_ok() {
            return self.transactions.update_status(tx_id, TransactionStatus::Resolved).await.with_context(|| {
                format!(
                    "Failed to update transaction status: resolve for client_id: {client_id:?} and tx_id: {tx_id:?}",
                )
            });
        }

        res
    }

    async fn handle_chargeback(&self, cb: Chargeback, direction: Direction) -> Result<()> {
        let client_id = cb.client_id;
        let tx_id = cb.tx_id;

        let res = async move {
            let acc = self.accounts.get(client_id).await;

            let Ok(Some(mut acc)) = acc else {
                return Err(anyhow!("Failed to get account from AccountRepo for client_id: {client_id:?}"));
            };

            acc.chargeback(direction).with_context(|| format!("Failed to perform the chargeback for a client_id: {client_id:?} and tx_id: {tx_id:?}"))?;

            self.accounts.upsert(acc).await.with_context(|| {
                format!("Failed to upsert account in AccountRepo: {acc:?}")
            })
        }
        .await;

        if res.is_ok() {
            return self.transactions.update_status(tx_id, TransactionStatus::Chargedback).await.with_context(|| {
                format!(
                    "Failed to update transaction status: resolve for client_id: {client_id:?} and tx_id: {tx_id:?}",
                )
            });
        }

        res
    }
}

fn prevent_replay_attack(maybe_tx: Option<&Transaction>) -> Result<()> {
    if let Some(tx) = maybe_tx
        && tx.status == TransactionStatus::Processed
    {
        let tx_id = tx.id;
        let client = tx.client_id;

        bail!(
            "Failed to process tx: {tx_id:?} for client {client:?} because the referenced transaction either does not exist or does not have the required status: TransactionStatus::Processed"
        )
    }

    Ok(())
}

fn tx_exists_and_has_been_processed(maybe_tx: Option<&Transaction>) -> Result<&Transaction> {
    ensure_tx_and_status(maybe_tx, TransactionStatus::Processed)
}

fn tx_exists_and_has_been_disputed(maybe_tx: Option<&Transaction>) -> Result<&Transaction> {
    ensure_tx_and_status(maybe_tx, TransactionStatus::Disputed)
}

fn tx_exists_and_has_been_resolved(maybe_tx: Option<&Transaction>) -> Result<&Transaction> {
    ensure_tx_and_status(maybe_tx, TransactionStatus::Resolved)
}

fn ensure_tx_and_status(
    maybe_tx: Option<&Transaction>,
    expected_status: TransactionStatus,
) -> Result<&Transaction> {
    if let Some(tx) = maybe_tx
        && tx.status == expected_status
    {
        Ok(tx)
    } else {
        Err(anyhow!(
            "Failed to process tx because the referenced transaction either does not exist or failed"
        ))
    }
}
