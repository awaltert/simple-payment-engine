use std::io::{Read, Write};

use anyhow::{Context, Result, anyhow};
use csv::{Reader, ReaderBuilder, Trim};
use futures::stream::{self, FusedStream, StreamExt};
use serde::Deserialize;
use tracing::error;

use crate::models::NonNegativeDecimal;
use crate::models::account::Account;
use crate::models::client::ClientId;
use crate::models::transaction::{
    Chargeback, Deposit, Dispute, Resolve, TransactionId, TxRecord, Withdrawal,
};

/// The CsvDecoder plays an important role in the system design.
///
/// It will be constructed with a reader, which is something that implements [std::io::Read]. This makes it very [flexible](https://doc.rust-lang.org/std/io/trait.Read.html#implementors) to use.
/// The implementation is two-fold. The construction part in necessary for the reader to have a lifetime that is longer than any consumer of the [FusedStream].
///
/// There are two types involved for deserializing the CSV encoded transactions. Only [Deposit] and [Withdrawal] transaction contain an amount, whereas [Resolve] and [Chargeback] don't.
/// Rather than using an [Option<Decimal>] in the entire codebase, I designed an enum [TxRecord] encoding the type information in its variants. This has the advantage, that there is only an amount when expected.
/// Unfortunately, a direct deserialization utilizing serdes internally tagged enum feature didn't work. Thus, I decided to introduce an internal struct [DeTxRecord] that is being using for direct deserialization.
///
/// Afterwards, we are using a `TryFrom` implementation to convert from [DeTxRecord] into [TxRecord], which denotes the public interface of the fused stream.
///
/// I decided to use a fused stream to avoid any undesired undefined behavior, if an consumer calls next, after a `None` has been received. See the documentation for the [StreamExt::fuse] method
pub struct CsvDecoder<R> {
    reader: Reader<R>,
}

impl<R: Read> CsvDecoder<R> {
    pub fn new(reader: R) -> Self {
        // The builder is configured to fulfill the following requirements:
        // - trim whitespace from header and values
        // - a header row is expected to be always present
        // - the expected delimiter
        let reader = ReaderBuilder::new()
            .trim(Trim::All)
            .has_headers(true)
            .delimiter(b',')
            .from_reader(reader);

        Self { reader }
    }

    pub fn decode_tx(&mut self) -> impl FusedStream<Item = TxRecord> {
        let records = self
            .reader
            .deserialize::<DeTxRecord>()
            .map(|rec| rec.context("Failed to deserialize CSV record into DeTxRecord"))
            .filter_map(|deserialized| {
                deserialized
                    .inspect_err(|err| {
                        error!("Failed to deserialize DeTxRecord from CsvRecord: {err:?}");
                    })
                    .and_then(TxRecord::try_from)
                    .inspect_err(|err| {
                        tracing::error!("Failed to convert DeTxRecord to TxRecord: {err:?}");
                    })
                    .ok()
            });
        // Using a fused stream to avoid undefined behavior
        stream::iter(records).fuse()
    }
}

pub struct CsvEncoder;

impl CsvEncoder {
    pub fn encode_balances<W: Write>(sink: W, accounts: &[Account]) -> Result<()> {
        let mut writer = csv::Writer::from_writer(sink);
        for acc in accounts {
            writer
                .serialize(acc)
                .context("Failed to serialize account to stdout")?;
        }
        writer.flush().context("Failed to flush the writer")?;

        Ok(())
    }
}

/// This internal type is simple workaround because serdes internally tagged enum serialization didnt work as expected.
#[derive(Debug, Deserialize)]
struct DeTxRecord {
    #[serde(rename = "type")]
    pub tx_type: DeTxType,

    #[serde(rename = "client")]
    pub client_id: ClientId,

    #[serde(rename = "tx")]
    pub tx_id: TransactionId,

    pub amount: Option<NonNegativeDecimal>,
}

/// This type converter is simple workaround because serdes internally tagged enum serialization didnt work as expected.
impl TryFrom<DeTxRecord> for TxRecord {
    type Error = anyhow::Error;

    fn try_from(deserialized: DeTxRecord) -> Result<Self, Self::Error> {
        let record = match deserialized.tx_type {
            DeTxType::Deposit => TxRecord::Deposit(Deposit {
                client_id: deserialized.client_id,
                tx_id: deserialized.tx_id,
                amount: deserialized.amount.with_context(|| {
                    anyhow!("Deserialized deposit does not contain an amount, which was expected")
                })?,
            }),

            DeTxType::Withdrawal => TxRecord::Withdrawal(Withdrawal {
                client_id: deserialized.client_id,
                tx_id: deserialized.tx_id,
                amount: deserialized.amount.with_context(|| {
                    anyhow!(
                        "Deserialized withdrawal does not contain an amount, which was expected"
                    )
                })?,
            }),

            DeTxType::Dispute => TxRecord::Dispute(Dispute {
                client_id: deserialized.client_id,
                tx_id: deserialized.tx_id,
            }),

            DeTxType::Resolve => TxRecord::Resolve(Resolve {
                client_id: deserialized.client_id,
                tx_id: deserialized.tx_id,
            }),

            DeTxType::Chargeback => TxRecord::Chargeback(Chargeback {
                client_id: deserialized.client_id,
                tx_id: deserialized.tx_id,
            }),
        };

        Ok(record)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum DeTxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}
