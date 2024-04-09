use super::{events_processor::EventsProcessor, user_transaction_processor::UserTransactionProcessor, ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::default_models::transactions::TransactionModel,
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, PgDbPool},
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use std::fmt::Debug;
use tracing::error;

pub struct MercatoProcessor {
    connection_pool: PgDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
    events_processor: EventsProcessor,
    user_transaction_processor: UserTransactionProcessor,
}

impl MercatoProcessor {
    pub fn new(connection_pool: PgDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        let events_processor_connection_pool = connection_pool.clone();
        let user_transaction_processor_connection_pool = connection_pool.clone();
        let events_processor_per_table_chunk_sizes = per_table_chunk_sizes.clone();
        let user_transaction_processor_per_table_chunk_sizes = per_table_chunk_sizes.clone();
        Self {
            connection_pool,
            per_table_chunk_sizes,
            events_processor: EventsProcessor::new(events_processor_connection_pool, events_processor_per_table_chunk_sizes),
            user_transaction_processor: UserTransactionProcessor::new(user_transaction_processor_connection_pool, user_transaction_processor_per_table_chunk_sizes),
        }
    }
}

impl Debug for MercatoProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: PgDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    txns: &[TransactionModel],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting into \"transactions\"",
    );

    let txns_res = execute_in_chunks(
        conn.clone(),
        insert_transactions_query,
        txns,
        get_config_table_chunk_size::<TransactionModel>("transactions", per_table_chunk_sizes),
    ).await;

     match txns_res {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(
                start_version = start_version,
                end_version = end_version,
                processor_name = name,
                error = ?e,
                "[Parser] Error inserting transactions into \"transactions\"",
            );
            Ok(())
        },
    }

}

fn insert_transactions_query(
    items_to_insert: Vec<TransactionModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::transactions::dsl::*;

    (
        diesel::insert_into(schema::transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                payload_type.eq(excluded(payload_type)),
            )),
        None,
    )
}


#[async_trait]
impl ProcessorTrait for MercatoProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::MercatoProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        tracing::info!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Processing new transactions",
        );
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();
        let (
            txns,
            _,
            _,
            _
        ) = TransactionModel::from_transactions(&transactions);
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &txns,
            &self.per_table_chunk_sizes,
        )
        .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        let result = match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timestamp,
            }),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
        };
        tracing::trace!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Processing events",
        );
        self.events_processor.process_transactions(transactions.clone(), start_version, end_version, None).await?;
        
        tracing::trace!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Processing user transactions",
        );
        self.user_transaction_processor.process_transactions(transactions, start_version, end_version, None).await?;
        result
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

