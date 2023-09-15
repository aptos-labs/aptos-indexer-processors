// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::default_models::{
        events::Event,
        transactions::{TransactionDetail, TransactionModel},
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    utils::database::PgDbPool,
};
use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use google_cloud_googleapis::spanner::v1::Mutation;
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    mutation::insert_or_update,
    statement::ToKind,
};
use serde::Serialize;
use std::fmt::Debug;
use time::OffsetDateTime;
use tracing::info;

pub const NAME: &str = "default_processor2";
pub const CHUNK_SIZE: usize = 1000;

pub struct DefaultProcessor2 {
    connection_pool: PgDbPool,
    spanner_db: String,
}

impl DefaultProcessor2 {
    pub fn new(connection_pool: PgDbPool, spanner_db: String) -> Self {
        tracing::info!("init DefaultProcessor2");
        Self {
            connection_pool,
            spanner_db,
        }
    }
}

impl Debug for DefaultProcessor2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultProcessor2 {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for DefaultProcessor2 {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        // Create spanner client
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(self.spanner_db.clone(), config).await?;

        let mut mutations = Vec::new();

        for transaction in transactions {
            let (txn, txn_detail, events, write_set_change, wsc_detail) =
                TransactionModel::from_transaction(&transaction);
            let block_height = txn.block_height;

            // Scary raw transaction big JSON table (direct proto -> JSON)
            mutations.push(insert_or_update(
                "transactions_raw",
                &["transaction_version", "transaction"],
                &[&(transaction.version as i64), &serialize_json(&transaction)],
            ));

            // The 3 slightly normalized tables we were talking about
            // Transactions
            mutations.push(transaction_to_mutation(txn, txn_detail)?);

            // Events
            for event in events {
                mutations.push(events_to_mutation(event));
            }

            // WriteSetChanges
            for (write_set_change, write_set_change_detail) in
                write_set_change.iter().zip(wsc_detail.iter())
            {
                mutations.push(write_set_change_to_mutation(
                    write_set_change,
                    write_set_change_detail,
                    block_height,
                ));
            }
        }

        let chunks = mutations.chunks(CHUNK_SIZE).map(|chunk| chunk.to_vec());
        for chunk in chunks {
            if let Some(commit_timestamp) = client.apply(chunk).await? {
                info!(
                    spanner_info = self.spanner_db.clone(),
                    commit_timestamp = commit_timestamp.seconds,
                    start_version = start_version,
                    end_version = end_version,
                    "Inserted or updated transaction",
                );
            }
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

fn serialize_json<T: Serialize>(value: T) -> String {
    serde_json::to_string(&value).unwrap_or_default()
}

fn transaction_to_mutation(
    txn: TransactionModel,
    txn_detail: Option<TransactionDetail>,
) -> anyhow::Result<Mutation> {
    let mut columns = vec![
        "transaction_version",
        "block_height",
        "hash",
        "transaction_type",
        "payload",
        "state_change_hash",
        "event_root_hash",
        "state_checkpoint_hash",
        "gas_used",
        "success",
        "vm_status",
        "accumulator_root_hash",
        "num_events",
        "num_write_set_changes",
        "epoch",
    ];

    let payload = serialize_json(&txn.payload);
    let mut values: Vec<&dyn ToKind> = vec![
        &txn.version,
        &txn.block_height,
        &txn.hash,
        &txn.type_,
        &payload,
        &txn.state_change_hash,
        &txn.event_root_hash,
        &txn.state_checkpoint_hash,
        &txn.gas_used,
        &txn.success,
        &txn.vm_status,
        &txn.accumulator_root_hash,
        &txn.num_events,
        &txn.num_write_set_changes,
        &txn.epoch,
    ];

    if let Some(details) = txn_detail {
        match details {
            TransactionDetail::User(user_tx, sig) => {
                columns.extend(vec![
                    "parent_signature_type",
                    "sender",
                    "sequence_number",
                    "max_gas_amount",
                    "expiration_timestamp_secs",
                    "gas_unit_price",
                    "timestamp",
                    "entry_function_id_str",
                    "signature",
                ]);

                let expiration_timestamp_secs = OffsetDateTime::from_unix_timestamp(
                    user_tx.expiration_timestamp_secs.timestamp(),
                )?;
                let timestamp = OffsetDateTime::from_unix_timestamp(user_tx.timestamp.timestamp())?;
                let signature = serialize_json(sig);
                let additional_vals: Vec<&dyn ToKind> = vec![
                    &user_tx.parent_signature_type,
                    &user_tx.sender,
                    &user_tx.sequence_number,
                    &user_tx.max_gas_amount,
                    &expiration_timestamp_secs,
                    &user_tx.gas_unit_price,
                    &timestamp,
                    &user_tx.entry_function_id_str,
                    &signature,
                ];
                values.extend(additional_vals);
                return Ok(insert_or_update("transactions", &columns, &values));
            },
            TransactionDetail::BlockMetadata(block_metadata_tx) => {
                columns.extend(vec![
                    "id",
                    "round",
                    "previous_block_votes_bitvec",
                    "proposer",
                    "failed_proposer_indices",
                ]);

                let previous_block_votes_bitvec =
                    serialize_json(&block_metadata_tx.previous_block_votes_bitvec);
                let failed_proposer_indices =
                    serialize_json(&block_metadata_tx.failed_proposer_indices);
                let addition_vals: Vec<&dyn ToKind> = vec![
                    &block_metadata_tx.id,
                    &block_metadata_tx.round,
                    &previous_block_votes_bitvec,
                    &block_metadata_tx.proposer,
                    &failed_proposer_indices,
                ];
                values.extend(addition_vals);
                return Ok(insert_or_update("transactions", &columns, &values));
            },
        }
    }
    Ok(insert_or_update("transactions", &columns, &values))
}

fn events_to_mutation(event: Event) -> Mutation {
    let columns = vec![
        "sequence_number",
        "creation_number",
        "account_address",
        "transaction_version",
        "transaction_block_height",
        "type",
        "data",
        "event_index",
    ];

    let data = serialize_json(&event.data);
    let values: Vec<&dyn ToKind> = vec![
        &event.sequence_number,
        &event.creation_number,
        &event.account_address,
        &event.transaction_version,
        &event.transaction_block_height,
        &event.type_,
        &data,
        &event.event_index,
    ];
    insert_or_update("events", &columns, &values)
}

fn write_set_change_to_mutation(
    write_set_change: &WriteSetChangeModel,
    write_set_change_detail: &WriteSetChangeDetail,
    block_height: i64,
) -> Mutation {
    let mut columns = vec![
        "transaction_version",
        "index",
        "hash",
        "transaction_block_height",
        "write_set_change_type",
        "address",
    ];

    let mut values: Vec<&dyn ToKind> = vec![
        &write_set_change.transaction_version,
        &write_set_change.index,
        &write_set_change.hash,
        &block_height,
        &write_set_change.type_,
        &write_set_change.address,
    ];

    match write_set_change_detail {
        WriteSetChangeDetail::Module(move_module) => {
            columns.extend(vec!["bytecode", "exposed_functions", "friends", "structs"]);

            let exposed_functions = serialize_json(&move_module.exposed_functions);
            let friends = serialize_json(&move_module.friends);
            let structs = serialize_json(&move_module.structs);
            let additional_values: Vec<&dyn ToKind> = vec![
                &move_module.bytecode,
                &exposed_functions,
                &friends,
                &structs,
            ];
            values.extend(additional_values);

            insert_or_update("write_set_changes", &columns, &values)
        },
        WriteSetChangeDetail::Resource(move_resource) => {
            columns.extend(vec![
                "name",
                "module",
                "generic_type_params",
                "data",
                "state_key_hash",
            ]);

            let generic_type_params = serialize_json(&move_resource.generic_type_params);
            let data = serialize_json(&move_resource.data);
            let additional_values: Vec<&dyn ToKind> = vec![
                &move_resource.name,
                &move_resource.module,
                &generic_type_params,
                &data,
                &move_resource.state_key_hash,
            ];
            values.extend(additional_values);

            insert_or_update("write_set_changes", &columns, &values)
        },
        WriteSetChangeDetail::Table(table_item, _, _) => {
            // Should I be using TableItem or CurrentTableItem?
            columns.extend(vec![
                "key",
                "table_handle",
                "decoded_key",
                "decoded_value",
                "is_deleted",
            ]);

            let decoded_key = serialize_json(&table_item.decoded_key);
            let decoded_value = serialize_json(&table_item.decoded_value);
            let additional_values: Vec<&dyn ToKind> = vec![
                &table_item.key,
                &table_item.table_handle,
                &decoded_key,
                &decoded_value,
                &table_item.is_deleted,
            ];
            values.extend(additional_values);

            insert_or_update("write_set_changes", &columns, &values)
        },
    }
}
