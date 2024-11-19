use crate::utils::database::ArcDbPool;
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{write_set_change::Change, Transaction},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::{convert::standardize_address, errors::ProcessorError},
};
use async_trait::async_trait;
use processor::{
    db::postgres::models::{
        object_models::{
            v2_object_utils::{
                ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
            },
            v2_objects::{CurrentObject, Object},
        },
        resources::FromWriteResource,
    },
    worker::TableFlags,
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
        let mut conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get connection from pool: {:?}", e),
                query: None,
            })?;
        let query_retries = self.query_retries;
        let query_retry_delay_ms = self.query_retry_delay_ms;

        // Moving object handling here because we need a single object
        // map through transactions for lookups
        let mut all_objects = vec![];
        let mut all_current_objects = AHashMap::new();
        let mut object_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();

        for txn in &transactions.data {
            let txn_version = txn.version as i64;
            let changes = &txn
                .info
                .as_ref()
                .unwrap_or_else(|| {
                    panic!(
                        "Transaction info doesn't exist! Transaction {}",
                        txn_version
                    )
                })
                .changes;

            // First pass to get all the object cores
            for wsc in changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());
                    if let Some(object_with_metadata) =
                        ObjectWithMetadata::from_write_resource(wr).unwrap()
                    {
                        // Object core is the first struct that we need to get
                        object_metadata_helper.insert(address.clone(), ObjectAggregatedData {
                            object: object_with_metadata,
                            token: None,
                            fungible_asset_store: None,
                            // The following structs are unused in this processor
                            fungible_asset_metadata: None,
                            aptos_collection: None,
                            fixed_supply: None,
                            unlimited_supply: None,
                            concurrent_supply: None,
                            property_map: None,
                            transfer_events: vec![],
                            untransferable: None,
                            fungible_asset_supply: None,
                            concurrent_fungible_asset_supply: None,
                            concurrent_fungible_asset_balance: None,
                            token_identifier: None,
                        });
                    }
                }
            }

            // Second pass to construct the object data
            for (index, wsc) in changes.iter().enumerate() {
                let index: i64 = index as i64;
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(inner) => {
                        if let Some((object, current_object)) = &Object::from_write_resource(
                            inner,
                            txn_version,
                            index,
                            &object_metadata_helper,
                        )
                        .unwrap()
                        {
                            all_objects.push(object.clone());
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    Change::DeleteResource(inner) => {
                        // Passing all_current_objects into the function so that we can get the owner of the deleted
                        // resource if it was handled in the same batch
                        if let Some((object, current_object)) = Object::from_delete_resource(
                            inner,
                            txn_version,
                            index,
                            &all_current_objects,
                            &mut conn,
                            query_retries,
                            query_retry_delay_ms,
                        )
                        .await
                        .unwrap()
                        {
                            all_objects.push(object.clone());
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    _ => {},
                };
            }
        }

        // Sort by PK
        let mut all_current_objects = all_current_objects
            .into_values()
            .collect::<Vec<CurrentObject>>();
        all_current_objects.sort_by(|a, b| a.object_address.cmp(&b.object_address));

        if self.deprecated_tables.contains(TableFlags::OBJECTS) {
            all_objects.clear();
        }

        Ok(Some(TransactionContext {
            data: (all_objects, all_current_objects),
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
