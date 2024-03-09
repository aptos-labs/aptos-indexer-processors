// Copyright © Aptos Foundation

use crate::{
    models::events_models::events::CachedEvent,
    utils::{counters::GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS, filter::EventFilter},
};
use aptos_in_memory_cache::{Cache, Incrementable, Ordered};
use futures::{stream::SplitSink, SinkExt};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use warp::filters::ws::{Message, WebSocket};

pub struct Stream<C: Cache<EventCacheKey, CachedEvent> + Ordered<EventCacheKey> + 'static> {
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<RwLock<EventFilter>>,
    cache: Arc<RwLock<C>>,
}

impl<C: Cache<EventCacheKey, CachedEvent> + Ordered<EventCacheKey> + 'static> Stream<C> {
    pub fn new(
        tx: SplitSink<WebSocket, Message>,
        filter: Arc<RwLock<EventFilter>>,
        cache: Arc<RwLock<C>>,
    ) -> Self {
        info!("Received WebSocket connection");
        Self { tx, filter, cache }
    }

    /// Maintains websocket connection and sends messages from channel
    pub async fn run(&mut self, starting_event: EventCacheKey) {
        let mut next_event = starting_event;
        loop {
            // Do not continue if filter is empty
            let filter = self.filter.read().await;
            let cache = self.cache.read().await;
            if !filter.is_empty() {
                // Try to get next event from cache
                if let Some(cached_event) = cache.get(&next_event) {
                    // Calculate what the next event to check would be first so we don't have to recalculate it later
                    let possible_next_event = next_event.next(&cached_event);

                    // If event is empty (transaction has no events), get next event
                    if cached_event.num_events_in_transaction == 0 {
                        next_event = possible_next_event;
                        continue;
                    }

                    // If filter matches, send event
                    let event = cached_event.event_stream_message;
                    if filter.accounts.contains(&event.account_address)
                        || filter.types.contains(&event.type_)
                    {
                        GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS.set({
                            use chrono::TimeZone;
                            let transaction_timestamp =
                                chrono::Utc.from_utc_datetime(&event.transaction_timestamp);
                            let transaction_timestamp =
                                std::time::SystemTime::from(transaction_timestamp);
                            std::time::SystemTime::now()
                                .duration_since(transaction_timestamp)
                                .unwrap_or_default()
                                .as_secs_f64()
                        });
                        let msg = serde_json::to_string(&event).unwrap_or_default();
                        if let Err(e) = self.tx.send(warp::ws::Message::text(msg)).await {
                            warn!(
                                error = ?e,
                                "[Event Stream] Failed to send message to WebSocket"
                            );
                            break;
                        }
                    }

                    next_event = possible_next_event;
                } else if next_event < cache.first_key().expect("Cache is empty") {
                    println!("next event is less than first key");
                    next_event = cache.last_key().expect("Cache is empty");
                } else {
                    println!("next event is greater than last key");
                    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
                }
            }
        }
    }
}

pub async fn spawn_stream<
    C: Cache<EventCacheKey, CachedEvent> + Ordered<EventCacheKey> + 'static,
>(
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<RwLock<EventFilter>>,
    cache: Arc<RwLock<C>>,
    starting_event: EventCacheKey,
) {
    let mut stream = Stream::new(tx, filter, cache);
    stream.run(starting_event).await;
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct EventCacheKey {
    pub transaction_version: i64,
    pub event_index: i64,
}

impl EventCacheKey {
    pub fn new(transaction_version: i64, event_index: i64) -> Self {
        Self {
            transaction_version,
            event_index,
        }
    }
}

impl Incrementable<CachedEvent> for EventCacheKey {
    fn next(&self, event: &CachedEvent) -> Self {
        if event.event_stream_message.event_index + 1 >= event.num_events_in_transaction as i64 {
            EventCacheKey::new(self.transaction_version + 1, 0)
        } else {
            EventCacheKey::new(
                self.transaction_version,
                event.event_stream_message.event_index + 1,
            )
        }
    }
}

impl Ord for EventCacheKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.transaction_version == other.transaction_version {
            self.event_index.cmp(&other.event_index)
        } else {
            self.transaction_version.cmp(&other.transaction_version)
        }
    }
}

impl PartialOrd for EventCacheKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
