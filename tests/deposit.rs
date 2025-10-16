use claims::{assert_ok, assert_some};
use futures::stream::{self, StreamExt};
use rust_decimal::dec;

use toy_payment_engine::models::NonNegativeDecimal;
use toy_payment_engine::models::client::ClientId;
use toy_payment_engine::models::transaction::{
    Deposit, TransactionId, TransactionStatus, TransactionType, TxRecord,
};

use setup::Components;
use toy_payment_engine::prelude::{AccountRepository, TransactionRepository};

mod setup;

#[tokio::test]
async fn can_perform_deposit_for_new_client() {
    let Components {
        engine,
        accounts,
        transactions,
    } = Components::setup();

    // arrange
    let client_id = ClientId::new(1);
    let tx_id = TransactionId::new(1);

    let txs = [Deposit {
        client_id,
        tx_id,
        amount: NonNegativeDecimal::try_from(42).unwrap(),
    }]
    .into_iter()
    .map(TxRecord::from);

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
    assert_eq!(account.available, dec!(42), "unexpected available amount");
    assert_eq!(account.held, dec!(0), "unexpected held amount");
    assert_eq!(account.total, dec!(42), "unexpected total amount");
    assert!(!account.is_locked, "unexpected is_locked");

    let tx = transactions.get(tx_id).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id:?} to be present");
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Processed,
        "Unexpected tx_status"
    );
}

#[tokio::test]
async fn can_perform_multiple_deposits() {
    let Components {
        engine,
        accounts,
        transactions,
    } = Components::setup();

    // arrange
    let client_id = ClientId::new(1);
    let amount = NonNegativeDecimal::try_from(1).unwrap();
    let txs = (1..=10)
        .map(|tx_id| Deposit {
            client_id,
            tx_id: TransactionId::new(tx_id),
            amount,
        })
        .map(TxRecord::from);

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

    for tx_id in 1..=10 {
        let tx_id = TransactionId::new(tx_id);

        let tx = transactions.get(tx_id).await;
        let tx = assert_some!(tx, "Expected tx with id: {tx_id:?} to be present");
        assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
        assert_eq!(
            tx.status,
            TransactionStatus::Processed,
            "Unexpected tx_status"
        );
    }
}
