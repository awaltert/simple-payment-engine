use claims::{assert_ok, assert_some};
use futures::stream::{self, StreamExt};
use rust_decimal::dec;

use toy_payment_engine::models::NonNegativeDecimal;
use toy_payment_engine::models::client::ClientId;
use toy_payment_engine::models::transaction::{
    Deposit, TransactionId, TransactionStatus, TransactionType, TxRecord, Withdrawal,
};

use setup::Components;
use toy_payment_engine::prelude::{AccountRepository, TransactionRepository};

mod setup;

#[tokio::test]
async fn client_can_perform_withdrawal_if_sufficient_funds() {
    let Components {
        engine,
        accounts,
        transactions,
    } = Components::setup();

    // arrange
    let client_id = ClientId::new(1);
    let tx_id = TransactionId::new(1);
    let tx_id2 = TransactionId::new(2);

    let txs = [
        TxRecord::from(Deposit {
            client_id,
            tx_id,
            amount: NonNegativeDecimal::try_from(42).unwrap(),
        }),
        TxRecord::from(Withdrawal {
            client_id,
            tx_id: tx_id2,
            amount: NonNegativeDecimal::try_from(4).unwrap(),
        }),
    ]
    .into_iter();

    // act
    engine.process(stream::iter(txs).fuse()).await;

    // assert
    let account = accounts.get(client_id).await;
    let account = assert_ok!(account);
    let account = assert_some!(
        account,
        "An account for client_id: {client_id:?} should be present",
    );
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, dec!(38), "unexpected available amount");
    assert_eq!(account.held, dec!(0), "unexpected held amount");
    assert_eq!(account.total, dec!(38), "unexpected total amount");
    assert!(!account.is_locked, "unexpected is_locked");

    let tx = transactions.get(tx_id).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id:?} to be present");
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Processed,
        "Unexpected tx_status"
    );

    let tx = transactions.get(tx_id2).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id2:?} to be present");
    assert_eq!(
        tx.tx_type,
        TransactionType::Withdrawal,
        "Unexpected tx_types"
    );
    assert_eq!(
        tx.status,
        TransactionStatus::Processed,
        "Unexpected tx_status"
    );
}

#[tokio::test]
async fn client_cant_perform_withdrawal_if_insufficient_funds() {
    let Components {
        engine,
        accounts,
        transactions,
    } = Components::setup();

    // arrange
    let client_id = ClientId::new(1);
    let tx_id = TransactionId::new(1);
    let tx_id2 = TransactionId::new(2);

    let txs = [
        TxRecord::from(Deposit {
            client_id,
            tx_id,
            amount: NonNegativeDecimal::try_from(10).unwrap(),
        }),
        TxRecord::from(Withdrawal {
            client_id,
            tx_id: tx_id2,
            amount: NonNegativeDecimal::try_from(14).unwrap(),
        }),
    ]
    .into_iter();

    // act
    engine.process(stream::iter(txs).fuse()).await;

    // assert
    let account = accounts.get(client_id).await;
    let account = assert_ok!(account);
    let account = assert_some!(
        account,
        "An account for client_id: {client_id:?} should be present",
    );
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, dec!(10), "unexpected available amount");
    assert_eq!(account.held, dec!(0), "unexpected held amount");
    assert_eq!(account.total, dec!(10), "unexpected total amount");
    assert!(!account.is_locked, "unexpected is_locked");

    let tx = transactions.get(tx_id).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id:?} to be present");
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Processed,
        "Unexpected tx_status"
    );

    let tx = transactions.get(tx_id2).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id2:?} to be present");
    assert_eq!(
        tx.tx_type,
        TransactionType::Withdrawal,
        "Unexpected tx_types"
    );
    assert_eq!(tx.status, TransactionStatus::Failed, "Unexpected tx_status");
}
