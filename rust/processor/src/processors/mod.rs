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
};
use crate::utils::{
    counters::{GOT_CONNECTION_COUNT, UNABLE_TO_GET_CONNECTION_COUNT},
    database::{PgDbPool, PgPoolConnection},
};
use aptos_processor_sdk::processor::ProcessorTrait;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

/// This is an extension for all the processors included in this codebase that allows
/// them to access a DB connection.
#[async_trait]
#[enum_dispatch]
pub trait ProcessorStorageTrait: Send + Sync + Debug {
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

/// Given a config and a db pool, build a Arc<dyn ProcessorTrait>.
//
// As time goes on there might be other things that we need to provide to certain
// processors. As that happens we can revist whether this function (which tends to
// couple processors together based on their args) makes sense.
pub fn build_processor(config: &ProcessorConfig, db_pool: PgDbPool) -> Arc<dyn ProcessorTrait> {
    match config {
        ProcessorConfig::AccountTransactionsProcessor => {
            Arc::new(AccountTransactionsProcessor::new(db_pool))
        },
        ProcessorConfig::AnsProcessor(config) => {
            Arc::new(AnsProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::CoinProcessor => Arc::new(CoinProcessor::new(db_pool)),
        ProcessorConfig::DefaultProcessor => Arc::new(DefaultProcessor::new(db_pool)),
        ProcessorConfig::EventsProcessor => Arc::new(EventsProcessor::new(db_pool)),
        ProcessorConfig::FungibleAssetProcessor => Arc::new(FungibleAssetProcessor::new(db_pool)),
        ProcessorConfig::NftMetadataProcessor(config) => {
            Arc::new(NftMetadataProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::StakeProcessor => Arc::new(StakeProcessor::new(db_pool)),
        ProcessorConfig::TokenProcessor(config) => {
            Arc::new(TokenProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::TokenV2Processor => Arc::new(TokenV2Processor::new(db_pool)),
        ProcessorConfig::UserTransactionProcessor => {
            Arc::new(UserTransactionProcessor::new(db_pool))
        },
    }
}
