// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// Note: For enum_dispatch to work nicely, it is easiest to have the trait and the enum
// in the same file (ProcessorTrait and Processor).

// Note: For enum_dispatch to work nicely, it is easiest to have the trait and the enum
// in the same file (ProcessorTrait and Processor).

pub mod account_transactions_processor;
pub mod ans_processor;
pub mod coin_processor;
pub mod default_processor;
pub mod events_processor;
pub mod fungible_asset_processor;
pub mod nft_metadata_processor;
pub mod stake_processor;
pub mod token_processor;
pub mod token_v2_processor;
pub mod user_transaction_processor;
pub mod events_processor_v2;
pub mod transactions_processor_v2;
pub mod wsc_processor_v2;

use self::{
    account_transactions_processor::AccountTransactionsProcessor,
    ans_processor::{AnsProcessor, AnsProcessorConfig},
    coin_processor::CoinProcessor,
    default_processor::DefaultProcessor,
    events_processor::EventsProcessor,
    fungible_asset_processor::FungibleAssetProcessor,
    nft_metadata_processor::{NftMetadataProcessor, NftMetadataProcessorConfig},
    stake_processor::StakeProcessor,
    token_processor::{TokenProcessor, TokenProcessorConfig},
    token_v2_processor::TokenV2Processor,
    user_transaction_processor::UserTransactionProcessor,
    events_processor_v2::EventsProcessorV2,
    transactions_processor_v2::TransactionsProcessorV2,
    wsc_processor_v2::WscProcessorV2,
};
use crate::{
    models::processor_status::ProcessorStatus,
    schema::processor_status,
    utils::{
        counters::{GOT_CONNECTION_COUNT, UNABLE_TO_GET_CONNECTION_COUNT},
        database::{execute_with_better_error, PgDbPool, PgPoolConnection},
    },
};
use aptos_protos::transaction::v1::Transaction as ProtoTransaction;
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, prelude::*};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

type StartVersion = u64;
type EndVersion = u64;
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct ProcessingResult {
    pub start_version: StartVersion,
    pub end_version: EndVersion,
    pub processing_duration_in_secs: f64,
    pub db_insertion_duration_in_secs: f64,
}

/// Base trait for all processors
#[async_trait]
#[enum_dispatch]
pub trait ProcessorTrait: Send + Sync + Debug {
    fn name(&self) -> &'static str;

    /// Process all transactions including writing to the database
    async fn process_transactions(
        &self,
        transactions: Vec<ProtoTransaction>,
        start_version: u64,
        end_version: u64,
        db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult>;

    /// Gets a reference to the connection pool
    /// This is used by the `get_conn()` helper below
    fn connection_pool(&self) -> &PgDbPool;

    //* Below are helper methods that don't need to be implemented *//

    /// Gets the connection.
    /// If it was unable to do so (default timeout: 30s), it will keep retrying until it can.
    async fn get_conn(&self) -> PgPoolConnection {
        let pool = self.connection_pool();
        loop {
            match pool.get().await {
                Ok(conn) => {
                    GOT_CONNECTION_COUNT.inc();
                    return conn;
                },
                Err(err) => {
                    UNABLE_TO_GET_CONNECTION_COUNT.inc();
                    tracing::error!(
                        // todo bb8 doesn't let you read the connection timeout.
                        //"Could not get DB connection from pool, will retry in {:?}. Err: {:?}",
                        //pool.connection_timeout(),
                        "Could not get DB connection from pool, will retry. Err: {:?}",
                        err
                    );
                },
            };
        }
    }

    /// Store last processed version from database. We can assume that all previously processed
    /// versions are successful because any gap would cause the processor to panic
    async fn update_last_processed_version(&self, version: u64) -> anyhow::Result<()> {
        let mut conn = self.get_conn().await;
        let status =
            ProcessorStatus {
                processor: self.name().to_string(),
                last_success_version: version as i64,
            };
        execute_with_better_error(
            &mut conn,
            diesel::insert_into(processor_status::table)
                .values(&status)
                .on_conflict(processor_status::processor)
                .do_update()
                .set((
                    processor_status::last_success_version
                        .eq(excluded(processor_status::last_success_version)),
                    processor_status::last_updated.eq(excluded(processor_status::last_updated)),
                )),
            Some(" WHERE processor_status.last_success_version <= EXCLUDED.last_success_version "),
        )
        .await?;
        Ok(())
    }
}

/// This enum captures the configs for all the different processors that are defined.
/// The configs for each processor should only contain configuration specific to that
/// processor. For configuration that is common to all processors, put it in
/// IndexerGrpcProcessorConfig.
#[derive(Clone, Debug, Deserialize, Serialize, strum::IntoStaticStr, strum::EnumDiscriminants)]
#[serde(tag = "type", rename_all = "snake_case")]
// What is all this strum stuff? Let me explain.
//
// Previously we had consts called NAME in each module and a function called `name` on
// the ProcessorTrait. As such it was possible for this name to not match the snake case
// representation of the struct name. By using strum we can have a single source for
// processor names derived from the enum variants themselves.
//
// That's what this strum_discriminants stuff is, it uses macro magic to generate the
// ProcessorName enum based on ProcessorConfig. The rest of the derives configure this
// generation logic, e.g. to make sure we use snake_case.
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(
    derive(
        Deserialize,
        Serialize,
        strum::EnumVariantNames,
        strum::IntoStaticStr,
        strum::Display,
        clap::ValueEnum
    ),
    name(ProcessorName),
    clap(rename_all = "snake_case"),
    serde(rename_all = "snake_case"),
    strum(serialize_all = "snake_case")
)]
pub enum ProcessorConfig {
    AccountTransactionsProcessor,
    AnsProcessor(AnsProcessorConfig),
    CoinProcessor,
    DefaultProcessor,
    TransactionsProcessorV2,
    EventsProcessorV2,
    WscProcessorV2,
    EventsProcessor,
    FungibleAssetProcessor,
    NftMetadataProcessor(NftMetadataProcessorConfig),
    StakeProcessor,
    TokenProcessor(TokenProcessorConfig),
    TokenV2Processor,
    UserTransactionProcessor,
}

impl ProcessorConfig {
    /// Get the name of the processor config as a static str. This is a convenience
    /// method to access the derived functionality implemented by strum::IntoStaticStr.
    pub fn name(&self) -> &'static str {
        self.into()
    }
}

/// This enum contains all the processors defined in this crate. We use enum_dispatch
/// as it is more efficient than using dynamic dispatch (Box<dyn ProcessorTrait>) and
/// it enables nice safety checks like in we do in `test_processor_names_complete`.
#[enum_dispatch(ProcessorTrait)]
#[derive(Debug)]
// To ensure that the variants of ProcessorConfig and Processor line up, in the testing
// build path we derive EnumDiscriminants on this enum as well and make sure the two
// sets of variants match up in `test_processor_names_complete`.
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(
        derive(strum::EnumVariantNames),
        name(ProcessorDiscriminants),
        strum(serialize_all = "snake_case")
    )
)]
pub enum Processor {
    AccountTransactionsProcessor,
    AnsProcessor,
    CoinProcessor,
    DefaultProcessor,
    TransactionsProcessorV2,
    EventsProcessorV2,
    WscProcessorV2,
    EventsProcessor,
    FungibleAssetProcessor,
    NftMetadataProcessor,
    StakeProcessor,
    TokenProcessor,
    TokenV2Processor,
    UserTransactionProcessor,
}

#[cfg(test)]
mod test {
    use super::*;
    use strum::VariantNames;

    /// This test exists to make sure that when a new processor is added, it is added
    /// to both Processor and ProcessorConfig. To make sure this passes, make sure the
    /// variants are in the same order (lexicographical) and the names match.
    #[test]
    fn test_processor_names_complete() {
        assert_eq!(ProcessorName::VARIANTS, ProcessorDiscriminants::VARIANTS);
    }
}
