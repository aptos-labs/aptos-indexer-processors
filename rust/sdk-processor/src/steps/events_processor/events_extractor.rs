use crate::db::common::models::events_models::EventModel;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    steps::{async_step::AsyncRunType, AsyncStep},
    traits::{NamedStep, Processable},
    types::transaction_context::TransactionContext,
};
use async_trait::async_trait;
use rayon::prelude::*;

pub struct EventsExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for EventsExtractor {
    type Input = Transaction;
    type Output = EventModel;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Transaction>,
    ) -> Option<TransactionContext<EventModel>> {
        let events = item
            .data
            .par_iter()
            .map(|txn| {
                let mut events = vec![];
                let txn_version = txn.version as i64;
                let block_height = txn.block_height as i64;
                let txn_data = match txn.txn_data.as_ref() {
                    Some(data) => data,
                    None => {
                        tracing::warn!(
                            transaction_version = txn_version,
                            "Transaction data doesn't exist"
                        );
                        // PROCESSOR_UNKNOWN_TYPE_COUNT
                        //     .with_label_values(&["EventsProcessor"])
                        //     .inc();
                        return vec![];
                    },
                };
                let default = vec![];
                let raw_events = match txn_data {
                    TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                    TxnData::Genesis(tx_inner) => &tx_inner.events,
                    TxnData::User(tx_inner) => &tx_inner.events,
                    _ => &default,
                };

                let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
                events.extend(txn_events);
                events
            })
            .flatten()
            .collect::<Vec<EventModel>>();
        Some(TransactionContext {
            data: events,
            start_version: item.start_version,
            end_version: item.end_version,
            start_transaction_timestamp: item.start_transaction_timestamp,
            end_transaction_timestamp: item.end_transaction_timestamp,
            total_size_in_bytes: item.total_size_in_bytes,
        })
    }
}

impl AsyncStep for EventsExtractor {}

impl NamedStep for EventsExtractor {
    fn name(&self) -> String {
        "EventsExtractor".to_string()
    }
}
