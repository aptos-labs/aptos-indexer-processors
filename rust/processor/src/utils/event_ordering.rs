use super::stream::EventCacheKey;
use crate::models::events_models::events::{CachedEvent, EventModel, EventStreamMessage};
use aptos_in_memory_cache::{Cache, Ordered};
use kanal::AsyncReceiver;
use std::{
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};
use tokio::sync::{Mutex, RwLock};
use tracing::error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionEvents {
    pub transaction_version: i64,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub events: Vec<EventModel>,
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

pub struct EventOrdering<C: Cache<EventCacheKey, CachedEvent> + Ordered<EventCacheKey> + 'static> {
    rx: AsyncReceiver<TransactionEvents>,
    cache: Arc<RwLock<C>>,
}

impl<C: Cache<EventCacheKey, CachedEvent> + Ordered<EventCacheKey> + 'static> EventOrdering<C> {
    pub fn new(rx: AsyncReceiver<TransactionEvents>, cache: Arc<RwLock<C>>) -> Self {
        Self { rx, cache }
    }

    pub async fn run(&self, starting_version: i64) {
        let heap_arc_lock = Arc::new(Mutex::new(BinaryHeap::new()));
        let rx = self.rx.clone();

        let heap_push = heap_arc_lock.clone();
        let push_thread = tokio::spawn(async move {
            loop {
                let event = rx.recv().await.unwrap_or_else(|e| {
                    error!(
                        error = ?e,
                        "[Event Stream] Failed to receive message from channel"
                    );
                    panic!();
                });

                let mut heap_locked = heap_push.lock().await;
                heap_locked.push(event);
            }
        });

        let heap_pop = heap_arc_lock.clone();
        let next_transaction_version = AtomicI64::new(starting_version);
        let cache_mutex = self.cache.clone();
        let pop_thread = tokio::spawn(async move {
            loop {
                let mut heap_locked = heap_pop.lock().await;
                if let Some(transaction_events) = heap_locked.peek() {
                    if transaction_events.transaction_version
                        == next_transaction_version.load(Ordering::SeqCst)
                    {
                        let transaction_timestamp = transaction_events.transaction_timestamp;
                        let transaction_events = heap_locked.pop().unwrap_or_else(|| {
                            error!("Failed to pop events from the heap");
                            panic!();
                        });

                        let num_events = transaction_events.events.len();
                        let mut cache = cache_mutex.write().await;
                        if num_events == 0 {
                            // Add empty event if transaction doesn't have any events
                            cache.insert(
                                EventCacheKey::new(transaction_events.transaction_version, 0),
                                CachedEvent::empty(transaction_events.transaction_version),
                            );
                        } else {
                            // Add all events to cache
                            for event in transaction_events.events {
                                cache.insert(
                                    EventCacheKey::new(
                                        event.transaction_version,
                                        event.event_index,
                                    ),
                                    CachedEvent::from_event_stream_message(
                                        &EventStreamMessage::from_event(
                                            &event,
                                            transaction_timestamp,
                                        ),
                                        num_events,
                                    ),
                                );
                            }
                        }
                        next_transaction_version.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        tokio::select! {
            _ = push_thread => {},
            _ = pop_thread => {},
        }
    }
}
