// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use tracing::info;

use crate::{
    processors::{EndVersion, ProcessedVersions, StartVersion},
    query_models::{QueryModelBatch, QueryModelBatchTrait},
    utils::database::{PgDbPool, UpsertFilterLatestTransactionQuery},
};

const QUERY_CHUNK_SIZE: usize = 10;

pub struct QueryExecutorBatch {
    pub processor_name: String,
    pub start_version: StartVersion,
    pub end_version: EndVersion,
    // TODO: Look into using BoxableExpression or BoxedQuery
    // pub queries: Vec<UpsertFilterLatestTransactionQuery>,
}

pub struct QueryExecutor {
    query_executor_receiver: tokio::sync::mpsc::Receiver<QueryExecutorBatch>,
    gap_detector_sender: tokio::sync::mpsc::Sender<ProcessedVersions>,
    db_pool: PgDbPool,
    processor_name: String,
}

impl QueryExecutor {
    pub fn new(
        query_executor_receiver: tokio::sync::mpsc::Receiver<QueryExecutorBatch>,
        gap_detector_sender: tokio::sync::mpsc::Sender<ProcessedVersions>,
        db_pool: PgDbPool,
        processor_name: String,
    ) -> Self {
        Self {
            query_executor_receiver,
            gap_detector_sender,
            db_pool,
            processor_name,
        }
    }

    pub async fn run(&mut self) {
        // Get items from receiver
        // Build the query
        // Spawn threads to execute the query
        // Push to gap detector channel
    }
}

#[derive(Clone, Debug)]
pub struct QueryBuilderBatch {
    pub start_version: StartVersion,
    pub end_version: EndVersion,
    pub models: Vec<QueryModelBatch>,
}

pub struct QueryBuilder {
    query_builder_receiver: tokio::sync::mpsc::Receiver<QueryBuilderBatch>,
    query_executor_sender: tokio::sync::mpsc::Sender<QueryExecutorBatch>,
    processor_name: String,
}

impl QueryBuilder {
    pub fn new(
        query_builder_receiver: tokio::sync::mpsc::Receiver<QueryBuilderBatch>,
        query_executor_sender: tokio::sync::mpsc::Sender<QueryExecutorBatch>,
        processor_name: String,
    ) -> Self {
        Self {
            query_builder_receiver,
            query_executor_sender,
            processor_name,
        }
    }

    pub async fn run(&mut self) {
        // Get items from receiver
        // Build the query
        // Push to db executor channel
        loop {
            // Get items from receiver
            let batch = match self.query_builder_receiver.recv().await {
                Some(batch) => batch,
                None => {
                    info!(
                        processor_name = self.processor_name,
                        "[Parser] Query builder channel has been closed"
                    );
                    return;
                },
            };

            let batch_clone = batch.clone();
            // Build the queries
            // TODO: Use rayon
            tokio::spawn(async move {
                // let queries = batch
                //     .models
                //     .iter()
                //     .map(|model| model.build_query())
                //     .collect();
                for model in batch_clone.models {
                    let (query, where_clause) = model.build_query();
                    // let query_executor_batch = QueryExecutorBatch {
                    //     processor_name: self.processor_name.clone(),
                    //     start_version: batch.start_version,
                    //     end_version: batch.end_version,
                    //     query,
                    //     where_clause,
                    // };
                    // self.query_executor_sender.send(query_executor_batch).await.unwrap();
                }
            });
        }
    }
}
