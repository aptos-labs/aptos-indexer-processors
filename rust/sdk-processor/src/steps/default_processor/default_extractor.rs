use crate::db::common::models::events_models::events::EventModel;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use rayon::prelude::*;
use tracing::warn;

pub const MIN_TRANSACTIONS_PER_RAYON_JOB: usize = 64;

pub struct DefaultExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for DefaultExtractor {
    type Input = Vec<Transaction>;
    type Output = Vec<EventModel>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<EventModel>>>, ProcessorError> {
        let events = item
            .data
            .par_iter()
            .with_min_len(MIN_TRANSACTIONS_PER_RAYON_JOB)
            .map(|txn| {
                let mut events = vec![];
                let txn_version = txn.version as i64;
                let block_height = txn.block_height as i64;
                let txn_data = match txn.txn_data.as_ref() {
                    Some(data) => data,
                    None => {
                        warn!(
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
                    TxnData::Validator(tx_inner) => &tx_inner.events,
                    _ => &default,
                };

                let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
                events.extend(txn_events);
                events
            })
            .flatten()
            .collect::<Vec<EventModel>>();
        Ok(Some(TransactionContext {
            data: events,
            metadata: item.metadata,
        }))
    }
    // process for events
    // async fn process(
    //     &mut self,
    //     item: TransactionContext<Vec<Transaction>>,
    // ) -> Result<Option<TransactionContext<Vec<EventModel>>>, ProcessorError> {
    //     let events = item
    //         .data
    //         .par_iter()
    //         .with_min_len(MIN_TRANSACTIONS_PER_RAYON_JOB)
    //         .map(|txn| {
    //             let mut events = vec![];
    //             let txn_version = txn.version as i64;
    //             let block_height = txn.block_height as i64;
    //             let txn_data = match txn.txn_data.as_ref() {
    //                 Some(data) => data,
    //                 None => {
    //                     warn!(
    //                         transaction_version = txn_version,
    //                         "Transaction data doesn't exist"
    //                     );
    //                     // PROCESSOR_UNKNOWN_TYPE_COUNT
    //                     //     .with_label_values(&["EventsProcessor"])
    //                     //     .inc();
    //                     return vec![];
    //                 },
    //             };
    //             let default = vec![];
    //             let raw_events = match txn_data {
    //                 TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
    //                 TxnData::Genesis(tx_inner) => &tx_inner.events,
    //                 TxnData::User(tx_inner) => &tx_inner.events,
    //                 TxnData::Validator(tx_inner) => &tx_inner.events,
    //                 _ => &default,
    //             };

    //             let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
    //             events.extend(txn_events);
    //             events
    //         })
    //         .flatten()
    //         .collect::<Vec<EventModel>>();
    //     Ok(Some(TransactionContext {
    //         data: events,
    //         metadata: item.metadata,
    //     }))
    // }
}

impl AsyncStep for DefaultExtractor {}

impl NamedStep for DefaultExtractor {
    fn name(&self) -> String {
        "DefaultExtractor".to_string()
    }
}
