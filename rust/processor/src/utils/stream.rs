// Copyright © Aptos Foundation

use super::in_memory_cache::InMemoryCache;
use crate::{
    db::common::models::events_models::events::CachedEvents,
    utils::{counters::GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS, filter::EventFilter},
};
use futures::{stream::SplitSink, SinkExt, StreamExt};
use std::{fmt::Debug, sync::Arc};
use tokio::sync::Notify;
use tracing::{info, warn};
use warp::filters::ws::{Message, WebSocket};

pub struct Stream {
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<EventFilter>,
    cache: Arc<InMemoryCache>,
    filter_edit_notify: Arc<Notify>,
}

impl Stream {
    pub fn new(
        tx: SplitSink<WebSocket, Message>,
        filter: Arc<EventFilter>,
        cache: Arc<InMemoryCache>,
        filter_edit_notify: Arc<Notify>,
    ) -> Self {
        info!("Received WebSocket connection");
        Self {
            tx,
            filter,
            cache,
            filter_edit_notify,
        }
    }

    /// Maintains websocket connection and sends messages from channel
    pub async fn run(&mut self, starting_event: Option<i64>) {
        let cache = self.cache.clone();
        let mut stream = Box::pin(cache.get_stream(starting_event));
        while let Some(cached_events) = stream.next().await {
            if self.filter.is_empty() {
                self.filter_edit_notify.notified().await;
            }

            if let Err(e) = self.send_events(cached_events).await {
                warn!(
                    error = ?e,
                    "Error sending events to WebSocket"
                );
                break;
            }
        }

        if let Err(e) = self.tx.send(Message::text("Stream ended")).await {
            warn!("Error sending error message: {:?}", e);
        }

        if let Err(e) = self.tx.send(Message::close()).await {
            warn!("Error sending close message: {:?}", e);
        }
    }

    async fn send_events(&mut self, cached_events: Arc<CachedEvents>) -> anyhow::Result<()> {
        for event in cached_events.events.clone() {
            if self.filter.accounts.contains(&event.account_address)
                || self.filter.types.contains(&event.type_)
            {
                GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS.set({
                    use chrono::TimeZone;
                    let transaction_timestamp =
                        chrono::Utc.from_utc_datetime(&event.transaction_timestamp);
                    let transaction_timestamp = std::time::SystemTime::from(transaction_timestamp);
                    std::time::SystemTime::now()
                        .duration_since(transaction_timestamp)
                        .unwrap_or_default()
                        .as_secs_f64()
                });
                let msg = serde_json::to_string(&event).unwrap_or_default();
                info!(
                    account_address = event.account_address,
                    transaction_version = event.transaction_version,
                    event_index = event.event_index,
                    event = msg,
                    "Sending event through WebSocket"
                );

                if let Err(e) = self.tx.send(Message::text(msg)).await {
                    warn!(
                        error = ?e,
                        "[Event Stream] Failed to send message to WebSocket"
                    );
                    return Err(anyhow::anyhow!(
                        "Failed to send message to WebSocket: {}",
                        e
                    ));
                }
            }
        }
        Ok(())
    }
}

pub async fn spawn_stream(
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<EventFilter>,
    cache: Arc<InMemoryCache>,
    starting_event: Option<i64>,
    filter_edit_notify: Arc<Notify>,
) {
    let mut stream = Stream::new(tx, filter, cache, filter_edit_notify);
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
