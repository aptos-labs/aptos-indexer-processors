// Copyright © Aptos Foundation

use crate::{
    models::events_models::events::CachedEvent,
    utils::{counters::GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS, filter::EventFilter},
};
use aptos_in_memory_cache::Cache;
use futures::{stream::SplitSink, SinkExt};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use warp::filters::ws::{Message, WebSocket};

pub struct Stream<C: Cache<(i64, i64), CachedEvent> + 'static> {
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<RwLock<EventFilter>>,
    cache: Arc<RwLock<C>>,
}

impl<C: Cache<(i64, i64), CachedEvent> + 'static> Stream<C> {
    pub fn new(
        tx: SplitSink<WebSocket, Message>,
        filter: Arc<RwLock<EventFilter>>,
        cache: Arc<RwLock<C>>,
    ) -> Self {
        info!("Received WebSocket connection");
        Self { tx, filter, cache }
    }

    /// Maintains websocket connection and sends messages from channel
    pub async fn run(&mut self, starting_event: (i64, i64)) {
        let mut next_event = starting_event;
        loop {
            // Do not continue if filter is empty
            let filter = self.filter.read().await;
            let cache = self.cache.read().await;
            if !filter.is_empty() {
                // Try to get next event from cache
                if let Some(cached_event) = cache.get(&next_event) {
                    // Calculate what the next event to check would be first so we don't have to recalculate it later
                    let possible_next_event = self.get_next_event_key(&cached_event).await;

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
                            warn!(error = ?e,
                                "[Event Stream] Failed to send message to WebSocket"
                            );
                            break;
                        }
                    }

                    next_event = possible_next_event;
                }
                // else if next_event < cache.first_key().expect("Cache is empty") {
                //     next_event = cache.last_key().expect("Cache is empty");
                // }
            }
        }
    }

    async fn get_next_event_key(&self, event: &CachedEvent) -> (i64, i64) {
        if event.event_stream_message.event_index + 1 >= event.num_events_in_transaction as i64 {
            (event.event_stream_message.transaction_version + 1, 0)
        } else {
            (
                event.event_stream_message.transaction_version,
                event.event_stream_message.event_index + 1,
            )
        }
    }
}

pub async fn spawn_stream<C: Cache<(i64, i64), CachedEvent> + 'static>(
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<RwLock<EventFilter>>,
    cache: Arc<RwLock<C>>,
    starting_event: (i64, i64),
) {
    let mut stream = Stream::new(tx, filter, cache);
    stream.run(starting_event).await;
}
