use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::db::{
    common::models::event_models::raw_events::RawEvent,
    postgres::{models::events_models::events::EventModel, PostgresConvertible},
};
use rayon::prelude::*;

pub struct EventsExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for EventsExtractor {
    type Input = Vec<Transaction>;
    type Output = Vec<EventModel>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<EventModel>>>, ProcessorError> {
        let events: Vec<_> = item
            .data
            .par_iter()
            .map(|txn| RawEvent::from_transaction(txn, "EventsProcessor"))
            .flatten()
            .collect();
        let postgres_events = events.to_postgres();
        Ok(Some(TransactionContext {
            data: postgres_events,
            metadata: item.metadata,
        }))
    }
}

impl AsyncStep for EventsExtractor {}

impl NamedStep for EventsExtractor {
    fn name(&self) -> String {
        "EventsExtractor".to_string()
    }
}
