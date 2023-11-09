// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::utils::database::PgDbPool;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub const CHUNK_SIZE: usize = 1000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TournamentProcessorConfig {
    contract_address: String,
}

pub struct TournamentProcessor {
    connection_pool: PgDbPool,
    chain_id: u8,
    config: TournamentProcessorConfig,
}

impl TournamentProcessor {
    pub fn new(connection_pool: PgDbPool, config: TournamentProcessorConfig) -> Self {
        tracing::info!("init TournamentProcessor");
        Self {
            connection_pool,
            chain_id: 0,
            config,
        }
    }

    pub fn set_chain_id(&mut self, chain_id: u8) {
        self.chain_id = chain_id;
    }
}

impl Debug for TournamentProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "TournamentProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for TournamentProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::TournamentProcessor.into()
    }

    async fn process_transactions(
        &self,
        _transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        // TODO: Process transactions
        println!("Contract address: {}", self.config.contract_address);
        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
