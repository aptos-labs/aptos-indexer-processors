use super::{counters::CACHE_SIZE_IN_BYTES, stream::EventCacheKey};
use crate::{
    models::events_models::events::{CachedEvent, EventOrder, EventStreamMessage},
    utils::counters::LAST_TRANSACTION_VERSION_IN_CACHE,
};
use ahash::AHashMap;
use aptos_in_memory_cache::StreamableOrderedCache;
use kanal::AsyncReceiver;
use std::sync::Arc;
use tracing::error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionEvents {
    pub transaction_version: i64,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub events: Vec<EventOrder>,
}

impl Ord for TransactionEvents {
    // Comparison must be reversed because BinaryHeap is a max-heap
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.transaction_version.cmp(&self.transaction_version)
    }
}

impl PartialOrd for TransactionEvents {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct EventOrdering<C: StreamableOrderedCache<EventCacheKey, CachedEvent> + 'static> {
    rx: AsyncReceiver<Vec<TransactionEvents>>,
    cache: Arc<C>,
}

impl<C: StreamableOrderedCache<EventCacheKey, CachedEvent> + 'static> EventOrdering<C> {
    pub fn new(rx: AsyncReceiver<Vec<TransactionEvents>>, cache: Arc<C>) -> Self {
        Self { rx, cache }
    }

    pub async fn run(&self, starting_version: i64) {
        let mut map = AHashMap::new();
        let rx = self.rx.clone();
        let mut next_transaction_version = starting_version;

        loop {
            let batch_events = rx.recv().await.unwrap_or_else(|e| {
                error!(
                    error = ?e,
                    "[Event Stream] Failed to receive message from channel"
                );
                panic!();
            });

            for events in batch_events {
                map.insert(events.transaction_version, events);
            }

            while let Some(transaction_events) = map.remove(&next_transaction_version) {
                let transaction_timestamp = transaction_events.transaction_timestamp;
                let num_events = transaction_events.events.len();
                if num_events == 0 {
                    // Add empty event if transaction doesn't have any events
                    self.cache.insert(
                        EventCacheKey::new(transaction_events.transaction_version, 0),
                        CachedEvent::empty(transaction_events.transaction_version),
                    );
                } else {
                    // Add all events to cache
                    for event in transaction_events.events {
                        self.cache.insert(
                            EventCacheKey::new(event.transaction_version, event.event_index),
                            CachedEvent::from_event_stream_message(
                                &EventStreamMessage::from_event_order(&event, transaction_timestamp),
                                num_events,
                            ),
                        );
                    }
                }
                LAST_TRANSACTION_VERSION_IN_CACHE
                    .set(self.cache.last_key().unwrap().transaction_version);
                CACHE_SIZE_IN_BYTES.set(self.cache.total_size() as i64);
                next_transaction_version += 1;
            }
        }
    }
}
