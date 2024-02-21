// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::{ProcessingResult, ProcessorName, ProcessorTrait},
    utils::database::PgDbPool,
};
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventFilterProcessorConfig {
    pub stream_ip: Option<String>,
}

pub struct EventFilterProcessor {
    connection_pool: PgDbPool,
}

impl EventFilterProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for EventFilterProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "EventFilterProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for EventFilterProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::EventFilterProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_insertion_duration_in_secs: 0.0,
            last_transaction_timstamp: transactions.last().unwrap().timestamp.clone(),
        })
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
