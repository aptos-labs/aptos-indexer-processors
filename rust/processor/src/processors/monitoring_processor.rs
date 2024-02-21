// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;

pub struct MonitoringProcessor {
    db_writer: crate::db_writer::DbWriter,
}

impl std::fmt::Debug for MonitoringProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool().state();
        write!(
            f,
            "{:} {{ connections: {:?}  idle_connections: {:?} }}",
            self.name(),
            state.connections,
            state.idle_connections
        )
    }
}

impl MonitoringProcessor {
    pub fn new(db_writer: crate::db_writer::DbWriter) -> Self {
        Self { db_writer }
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
            db_channel_insertion_duration_in_secs: 0.0,
            last_transaction_timestamp: transactions.last().unwrap().timestamp.clone(),
        })
    }

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
