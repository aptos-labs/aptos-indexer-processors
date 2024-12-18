use crate::utils::database::ArcDbPool;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::{
        common::models::object_models::raw_v2_objects::{
            CurrentObjectConvertible, ObjectConvertible,
        },
        postgres::models::object_models::v2_objects::{CurrentObject, Object},
    },
    processors::objects_processor::process_objects,
    utils::{database::DbContext, table_flags::TableFlags},
};

/// Extracts fungible asset events, metadata, balances, and v1 supply from transactions
pub struct ObjectsExtractor
where
    Self: Sized + Send + 'static,
{
    query_retries: u32,
    query_retry_delay_ms: u64,
    conn_pool: ArcDbPool,
    deprecated_tables: TableFlags,
}

impl ObjectsExtractor {
    pub fn new(
        query_retries: u32,
        query_retry_delay_ms: u64,
        conn_pool: ArcDbPool,
        deprecated_tables: TableFlags,
    ) -> Self {
        Self {
            query_retries,
            query_retry_delay_ms,
            conn_pool,
            deprecated_tables,
        }
    }
}

#[async_trait]
impl Processable for ObjectsExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<Object>, Vec<CurrentObject>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<(Vec<Object>, Vec<CurrentObject>)>>, ProcessorError> {
        let conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get connection from pool: {:?}", e),
                query: None,
            })?;
        let query_retries = self.query_retries;
        let query_retry_delay_ms = self.query_retry_delay_ms;
        let db_connection = DbContext {
            conn,
            query_retries,
            query_retry_delay_ms,
        };

        let (mut raw_all_objects, raw_all_current_objects) =
            process_objects(transactions.data, &mut Some(db_connection)).await;

        if self.deprecated_tables.contains(TableFlags::OBJECTS) {
            raw_all_objects.clear();
        }

        let postgres_all_objects: Vec<Object> =
            raw_all_objects.into_iter().map(Object::from_raw).collect();

        let postgres_all_current_objects: Vec<CurrentObject> = raw_all_current_objects
            .into_iter()
            .map(CurrentObject::from_raw)
            .collect();

        Ok(Some(TransactionContext {
            data: (postgres_all_objects, postgres_all_current_objects),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ObjectsExtractor {}

impl NamedStep for ObjectsExtractor {
    fn name(&self) -> String {
        "ObjectsExtractor".to_string()
    }
}
