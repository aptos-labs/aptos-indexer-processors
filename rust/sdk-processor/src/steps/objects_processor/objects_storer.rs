use crate::utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    self,
    db::postgres::models::object_models::v2_objects::{CurrentObject, Object},
    processors::objects_processor::{insert_current_objects_query, insert_objects_query},
};

pub struct ObjectsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl ObjectsStorer {
    pub fn new(conn_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        Self {
            conn_pool,
            per_table_chunk_sizes,
        }
    }
}

#[async_trait]
impl Processable for ObjectsStorer {
    type Input = (Vec<Object>, Vec<CurrentObject>);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(Vec<Object>, Vec<CurrentObject>)>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (objects, current_objects) = input.data;

        let io = execute_in_chunks(
            self.conn_pool.clone(),
            insert_objects_query,
            &objects,
            get_config_table_chunk_size::<Object>("objects", &self.per_table_chunk_sizes),
        );
        let co = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_objects_query,
            &current_objects,
            get_config_table_chunk_size::<CurrentObject>(
                "current_objects",
                &self.per_table_chunk_sizes,
            ),
        );
        let (io_res, co_res) = tokio::join!(io, co);
        for res in [io_res, co_res] {
            match res {
                Ok(_) => {},
                Err(e) => {
                    return Err(ProcessorError::DBStoreError {
                        message: format!(
                            "Failed to store versions {} to {}: {:?}",
                            input.metadata.start_version, input.metadata.end_version, e,
                        ),
                        query: None,
                    })
                },
            }
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for ObjectsStorer {}

impl NamedStep for ObjectsStorer {
    fn name(&self) -> String {
        "ObjectsStorer".to_string()
    }
}
