use claims::{assert_ok, assert_some};
use futures::stream::{self, StreamExt};
use rust_decimal::{Decimal, dec};

use toy_payment_engine::models::NonNegativeDecimal;
use toy_payment_engine::models::client::ClientId;
use toy_payment_engine::models::transaction::{
    Deposit, Dispute, Resolve, TransactionId, TransactionStatus, TransactionType, TxRecord,
    Withdrawal,
};

use setup::Components;
use toy_payment_engine::prelude::{AccountRepository, TransactionRepository};

mod setup;

#[tokio::test]
async fn can_resolve_a_deposit() {
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
        TxRecord::from(Deposit {
            client_id,
            tx_id: tx_id2,
            amount: NonNegativeDecimal::try_from(10).unwrap(),
        }),
        TxRecord::from(Dispute {
            client_id,
            tx_id: tx_id2,
        }),
        TxRecord::from(Resolve {
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
    assert_eq!(account.available, dec!(20), "unexpected available amount");
    assert_eq!(account.held, Decimal::ZERO, "unexpected held amount");
    assert_eq!(account.total, dec!(20), "unexpected total amount");
    assert!(!account.is_locked, "unexpected is_locked");

    let tx = transactions.get(tx_id2).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id2:?} to be present");
    assert_eq!(tx.tx_type, TransactionType::Deposit, "Unexpected tx_types");
    assert_eq!(
        tx.status,
        TransactionStatus::Resolved,
        "Unexpected tx_status"
    );
}

#[tokio::test]
async fn can_resolve_a_withdrawal() {
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
        TxRecord::from(Resolve {
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
    assert_eq!(account.available, dec!(10), "unexpected available amount");
    assert_eq!(account.held, Decimal::ZERO, "unexpected held amount");
    assert_eq!(account.total, dec!(10), "unexpected total amount");
    assert!(!account.is_locked, "unexpected is_locked");

    let tx = transactions.get(tx_id2).await;
    let tx = assert_some!(tx, "Expected tx with id: {tx_id2:?} to be present");
    assert_eq!(
        tx.tx_type,
        TransactionType::Withdrawal,
        "Unexpected tx_types"
    );
    assert_eq!(
        tx.status,
        TransactionStatus::Resolved,
        "Unexpected tx_status"
    );
}
