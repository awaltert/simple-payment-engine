use std::fs::File;
use std::io;
use std::sync::Arc;

use anyhow::{Context, Result};

use toy_payment_engine::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();

    let file_path = args
        .first()
        .context("Expected the input file path as first argument. Exiting...")?;
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open file with path: {}. Exiting", file_path))?;

    let mut csv_decoder = CsvDecoder::new(file);
    let accounts = Arc::new(InMemoryAccountRepository::new());
    let transactions = Arc::new(InMemoryTxRepository::new());
    let engine = PaymentEngine::new(Arc::clone(&accounts), Arc::clone(&transactions));

    engine.process(csv_decoder.decode_tx()).await;

    // Using an allocated vector here, due to time constraints
    // Ideally, streaming the accounts into the CsvEncoder would be better
    let balances = accounts.balances().await;

    CsvEncoder::encode_balances(io::stdout(), &balances)
        .context("Failed to encode balances as Csv")?;

    Ok(())
}
