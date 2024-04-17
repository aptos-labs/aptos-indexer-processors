// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::utils::database::ArcDbPool;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use std::fmt::Debug;

pub struct MonitoringProcessor {
    connection_pool: ArcDbPool,
}

impl MonitoringProcessor {
    pub fn new(connection_pool: ArcDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for MonitoringProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "MonitoringProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for MonitoringProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::MonitoringProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs: 0.0,
            db_insertion_duration_in_secs: 0.0,
            last_transaction_timestamp: transactions.last().unwrap().timestamp.clone(),
        })
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
