use crate::{
    db::common::models::events_models::EventModel,
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    steps::{async_step::AsyncRunType, AsyncStep},
    traits::{NamedStep, Processable},
    types::transaction_context::TransactionContext,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub struct EventsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
}

impl EventsStorer {
    pub fn new(conn_pool: ArcDbPool) -> Self {
        Self { conn_pool }
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
    ) -> Option<TransactionContext<EventModel>> {
        let per_table_chunk_sizes: AHashMap<String, usize> = AHashMap::new();
        let execute_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_events_query,
            &events.data,
            get_config_table_chunk_size::<EventModel>("events", &per_table_chunk_sizes),
        )
        .await;
        match execute_res {
            Ok(_) => {
                println!("Events stored successfully");
            },
            Err(e) => {
                println!("Failed to store events: {:?}", e);
            },
        }
        Some(events)
    }
}

impl AsyncStep for EventsStorer {}

impl NamedStep for EventsStorer {
    fn name(&self) -> String {
        "EventsStorer".to_string()
    }
}
