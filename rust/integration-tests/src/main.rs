// main.rs
use anyhow::Result;
use integration_tests::{TestContext, TestProcessorConfig};
mod config;

use clap::Parser;
use config::IndexerCliArgs;

#[allow(clippy::needless_return)]
#[tokio::main]
async fn main() -> Result<()> {
    let args = IndexerCliArgs::parse();
    args.run().await
}
