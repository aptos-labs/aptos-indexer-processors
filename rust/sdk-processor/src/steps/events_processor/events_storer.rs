use crate::{
    db::common::models::events_models::events::EventModel,
    processors::events_processor::EventsProcessorConfig,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use processor::schema;
use tracing::debug;

pub struct EventsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: EventsProcessorConfig,
}

impl EventsStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: EventsProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

fn insert_events_query(
    items_to_insert: Vec<EventModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::events::dsl::*;
    (
        diesel::insert_into(schema::events::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                indexed_type.eq(excluded(indexed_type)),
            )),
        None,
    )
}

#[async_trait]
impl Processable for EventsStorer {
    type Input = EventModel;
    type Output = EventModel;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        events: TransactionContext<EventModel>,
    ) -> Result<Option<TransactionContext<EventModel>>, ProcessorError> {
        // tracing::info!(
        //     start_version = events.start_version,
        //     end_version = events.end_version,
        //     step_name = self.name(),
        //     "Processing versions",
        // );
        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();
        let execute_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_events_query,
            &events.data,
            get_config_table_chunk_size::<EventModel>("events", &per_table_chunk_sizes),
        )
        .await;
        match execute_res {
            Ok(_) => {
                debug!(
                    "Events version [{}, {}] stored successfully",
                    events.start_version, events.end_version
                );
                Ok(Some(events))
            },
            Err(e) => Err(ProcessorError::DBStoreError {
                message: format!(
                    "Failed to store events versions {} to {}: {:?}",
                    events.start_version, events.end_version, e,
                ),
            }),
        }
    }
}

impl AsyncStep for EventsStorer {}

impl NamedStep for EventsStorer {
    fn name(&self) -> String {
        "EventsStorer".to_string()
    }
}
