// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
        default_models::transactions::TransactionModel,
        default_models2::{
            events::EventsCockroach,
            transactions::TransactionCockroach,
            write_set_changes::{
                WriteSetChangeCockroach, WriteSetChangeModule, WriteSetChangeResource,
                WriteSetChangeTable,
            },
        },
    },
    utils::database::PgDbPool,
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction as TransactionPB};
use async_trait::async_trait;
use std::fmt::Debug;
use tokio_postgres::{types::ToSql, Client, Error, NoTls};
use tracing::error;

pub const NAME: &str = "default_processor2";
pub const CHUNK_SIZE: usize = 1000;
const TRANSACTIONS_TABLE: &str = "transactions";
const EVENTS_TABLE: &str = "events";
const WRITE_SET_CHANGE_RESOURCE_TABLE: &str = "write_set_changes_resource";
const WRITE_SET_CHANGE_MODULE_TABLE: &str = "write_set_changes_module";
const WRITE_SET_CHANGE_TABLE_TABLE: &str = "write_set_changes_table";
const TRANSACTION_PK: &[&str] = &["transaction_version"];
const EVENT_PK: &[&str] = &["transaction_version", "event_index"];
const WRITE_SET_CHANGE_PK: &[&str] = &["transaction_version", "index"];

pub trait PGInsertable {
    fn get_insertable_sql_values(&self) -> (Vec<&str>, Vec<&(dyn ToSql + Sync)>);
}

pub struct DefaultProcessor2 {
    connection_pool: PgDbPool,
    cockroach_postgres_connection_string: String,
}

impl DefaultProcessor2 {
    pub fn new(connection_pool: PgDbPool, cockroach_postgres_connection_string: String) -> Self {
        tracing::info!("init DefaultProcessor2");
        Self {
            connection_pool,
            cockroach_postgres_connection_string,
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

async fn insert_to_table<T>(
    client: &mut Client,
    table_name: &str,
    pk: &[&str],
    data: Vec<T>,
) -> Result<(), Error>
where
    T: PGInsertable,
{
    let transaction = client.transaction().await?;

    for item in data {
        let (column_names, query_values) = item.get_insertable_sql_values();

        let placeholders: String = (1..=column_names.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<String>>()
            .join(", ");

        let pk_str = pk.join(", ");

        let query = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
            table_name,
            column_names.join(", "),
            placeholders,
            pk_str
        );

        transaction
            .execute(query.as_str(), query_values.as_slice())
            .await?;
    }

    transaction.commit().await?;
    Ok(())
}

async fn insert_transactions(
    txns: Vec<TransactionCockroach>,
    client: &mut Client,
) -> Result<(), Error> {
    insert_to_table(client, TRANSACTIONS_TABLE, TRANSACTION_PK, txns).await
}

async fn insert_events(events: Vec<EventsCockroach>, client: &mut Client,) -> Result<(), Error> {
    insert_to_table(client, EVENTS_TABLE, EVENT_PK, events).await
}

async fn insert_ws_changes(
    ws_changes: Vec<WriteSetChangeCockroach>,
    client: &mut Client,
) -> Result<(), Error> {
    let mut resource_transactions: Vec<WriteSetChangeResource> = Vec::new();
    let mut module_transactions: Vec<WriteSetChangeModule> = Vec::new();
    let mut table_transactions: Vec<WriteSetChangeTable> = Vec::new();

    for ws_change in ws_changes {
        match ws_change {
            WriteSetChangeCockroach::Resource(resource) => {
                resource_transactions.push(resource);
            },
            WriteSetChangeCockroach::Module(module) => {
                module_transactions.push(module);
            },
            WriteSetChangeCockroach::Table(table) => {
                table_transactions.push(table);
            },
        }
    }

    insert_to_table(
        client,
        WRITE_SET_CHANGE_RESOURCE_TABLE,
        WRITE_SET_CHANGE_PK,
        resource_transactions,
    )
    .await?;
    insert_to_table(
        client,
        WRITE_SET_CHANGE_MODULE_TABLE,
        WRITE_SET_CHANGE_PK,
        module_transactions,
    )
    .await?;
    insert_to_table(
        client,
        WRITE_SET_CHANGE_TABLE_TABLE,
        WRITE_SET_CHANGE_PK,
        table_transactions,
    )
    .await?;

    Ok(())
}

#[async_trait]
impl ProcessorTrait for DefaultProcessor2 {
    fn name(&self) -> &'static str {
        ProcessorName::DefaultProcessor2.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<TransactionPB>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let (mut client, connection) =
            tokio_postgres::connect(&self.cockroach_postgres_connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let txns: Vec<TransactionCockroach> =
            TransactionCockroach::from_transactions(&transactions);

        let mut events = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            let default = vec![];
            let raw_events = match txn_data {
                TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                TxnData::Genesis(tx_inner) => &tx_inner.events,
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => &default,
            };

            let txn_events = EventsCockroach::from_events(raw_events, txn_version, block_height);
            events.extend(txn_events);
        }

        let (_, _, write_set_changes, wsc_details) =
            TransactionModel::from_transactions(&transactions);
        let wscs = WriteSetChangeCockroach::from_wscs(write_set_changes, wsc_details);

        let insert_operations: Vec<(&str, Result<(), Error>)> = vec![
            ("transactions", insert_transactions(txns, &mut client).await),
            ("events", insert_events(events, &mut client).await),
            ("write set changes", insert_ws_changes(wscs, &mut client).await),
        ];

        for (operation_name, insert_result) in insert_operations {
            if let Err(err) = insert_result {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    "[Parser] Error inserting {} to db: {:?}",
                    operation_name,
                    err
                );

                bail!(format!(
                    "Error inserting {} to db. Processor {}. Start {}. End {}. Error {:?}",
                    operation_name,
                    self.name(),
                    start_version,
                    end_version,
                    err
                ));
            }
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

// async fn execute_txn<T, F>(
//     client: &mut Client,
//     op: F,
// ) -> Result<T, Error>
// where
//     F: Fn(&mut Transaction) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send>>,
// {
//     let mut txn = client.transaction().await?;

//     let result = async {
//         let mut sp = txn.savepoint("cockroach_restart").await?;
//         let commit_result: Result<T, Error> = Ok(
//             op(&mut sp)
//                 .await
//                 .map_err(|err| {
//                     if err
//                         .code()
//                         .map(|e| *e == SqlState::T_R_SERIALIZATION_FAILURE)
//                         .unwrap_or(false)
//                     {
//                         // Handle a specific error case here
//                     }
//                     err
//                 })?,
//         );
//         sp.commit().await?;
//         commit_result
//     }
//     .await;

//     match result {
//         Ok(r) => txn.commit().await.map(|_| r),
//         Err(e) => {
//             txn.rollback().await?;
//             Err(e)
//         }
//     }
// }
