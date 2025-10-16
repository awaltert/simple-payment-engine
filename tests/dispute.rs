use claims::{assert_ok, assert_some};
use futures::stream::{self, StreamExt};
use rust_decimal::{Decimal, dec};

use toy_payment_engine::models::NonNegativeDecimal;
use toy_payment_engine::models::client::ClientId;
use toy_payment_engine::models::transaction::{
    Deposit, Dispute, TransactionId, TransactionStatus, TransactionType, TxRecord, Withdrawal,
};

use setup::Components;
use toy_payment_engine::prelude::{AccountRepository, TransactionRepository};

mod setup;

#[tokio::test]
async fn can_perform_dispute_for_complete_available_amount() {
    let Components {
        engine,
        accounts,
        transactions,
    } = Components::setup();

    // arrange
    let client_id = ClientId::new(1);
    let tx_id = TransactionId::new(1);

    let txs = [
        TxRecord::from(Deposit {
            client_id,
            tx_id,
            amount: NonNegativeDecimal::try_from(42).unwrap(),
        }),
        TxRecord::from(Dispute { client_id, tx_id }),
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
    assert_eq!(
        account.available,
        Decimal::ZERO,
        "unexpected available amount"
    );
    assert_eq!(account.held, dec!(42), "unexpected held amount");
    assert_eq!(account.total, dec!(42), "unexpected total amount");
    assert!(!account.is_locked, "unexpected is_locked");

    let tx = transactions.get(tx_id).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id:?} to be present");
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Disputed,
        "Unexpected tx_status"
    );
}

#[tokio::test]
async fn can_perform_dispute_with_multiple_deposits() {
    let Components {
        engine,
        accounts,
        transactions,
    } = Components::setup();

    // arrange
    let client_id = ClientId::new(1);
    let tx_id = TransactionId::new(1);
    let tx_id2 = TransactionId::new(2);
    let tx_id3 = TransactionId::new(3);

    let txs = [
        TxRecord::from(Deposit {
            client_id,
            tx_id,
            amount: NonNegativeDecimal::try_from(1).unwrap(),
        }),
        TxRecord::from(Deposit {
            client_id,
            tx_id: tx_id2,
            amount: NonNegativeDecimal::try_from(2).unwrap(),
        }),
        TxRecord::from(Deposit {
            client_id,
            tx_id: tx_id3,
            amount: NonNegativeDecimal::try_from(5).unwrap(),
        }),
        TxRecord::from(Dispute {
            client_id,
            tx_id: tx_id2,
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
    assert_eq!(account.available, dec!(6), "unexpected available amount");
    assert_eq!(account.held, dec!(2), "unexpected held amount");
    assert_eq!(account.total, dec!(8), "unexpected total amount");
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
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Disputed,
        "Unexpected tx_status"
    );

    let tx = transactions.get(tx_id3).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id3:?} to be present");
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Processed,
        "Unexpected tx_status"
    );
}

#[tokio::test]
async fn cant_perform_dispute_if_more_than_available() {
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
            amount: NonNegativeDecimal::try_from(10).unwrap(),
        }),
        TxRecord::from(Dispute { client_id, tx_id }),
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
    assert_eq!(account.available, dec!(0), "unexpected available amount");
    assert_eq!(account.held, dec!(0), "unexpected held amount");
    assert_eq!(account.total, dec!(0), "unexpected total amount");
    assert!(!account.is_locked, "unexpected is_locked");

    let tx = transactions.get(tx_id).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id:?} to be present");
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Processed, // The status didn't change, thus the dispute has not been executed
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
async fn can_perform_dispute_of_withdrawal() {
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
            amount: NonNegativeDecimal::try_from(5).unwrap(),
        }),
        TxRecord::from(Dispute {
            client_id,
            tx_id: tx_id2,
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
    assert_eq!(account.available, dec!(5), "unexpected available amount");
    assert_eq!(account.held, dec!(5), "unexpected held amount");
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
    assert_eq!(
        tx.status,
        TransactionStatus::Disputed,
        "Unexpected tx_status"
    );
}
