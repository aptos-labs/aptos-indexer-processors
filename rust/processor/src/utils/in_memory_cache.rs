// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::db::common::models::events_models::events::CachedEvents;
use aptos_in_memory_cache::{caches::sync_mutex::SyncMutexCache, Cache, SizedCache};
use futures::{stream, Stream};
use get_size::GetSize;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};
use tokio::{sync::Notify, task::JoinHandle};

#[derive(Debug, Clone)]
pub struct InMemoryCacheMetadata {
    pub eviction_trigger_size_in_bytes: usize,
    pub target_size_in_bytes: usize,
    pub capacity: usize,
}

// #[derive(Debug)]
pub struct InMemoryCache {
    pub metadata: Arc<InMemoryCacheMetadata>,
    pub cache: Arc<SyncMutexCache<CachedEvents>>,
    pub eviction_notify: Arc<Notify>,
    // Metadata for stream
    pub head: Arc<AtomicI64>,
    pub tail: Arc<AtomicI64>,
    pub watermark: Arc<AtomicI64>,
    pub stream_notify: Arc<Notify>,
}

impl Cache<i64, CachedEvents> for InMemoryCache {
    fn get(&self, key: &i64) -> Option<Arc<CachedEvents>> {
        let key = &(*key as usize);
        self.cache.get(key).and_then(|entry| {
            if entry.key == *key {
                return Some(entry.value.clone());
            }
            None
        })
    }

    fn insert(&self, key: i64, value: CachedEvents) {
        let size_in_bytes = value.get_size();
        self.cache
            .insert_with_size(key as usize, Arc::new(value), size_in_bytes);

        // Fill pointers if cache was empty
        if self.head.load(Ordering::Relaxed) == -1 {
            self.head.store(key, Ordering::Relaxed);
        }

        if self.tail.load(Ordering::Relaxed) == -1 {
            self.tail.store(key, Ordering::Relaxed);
        }

        if self.watermark.load(Ordering::Relaxed) == -1 {
            self.watermark.store(key, Ordering::Relaxed);
        }

        // Since pointers have been filled, the unwraps below are safe
        // Update watermark to highest seen transaction version
        if key > self.watermark.load(Ordering::Relaxed) {
            self.watermark.store(key, Ordering::Relaxed);
        }

        // Update tail to the latest consecutive transaction version
        loop {
            let tail = self.tail.load(Ordering::Relaxed);
            let next_tail = self.get(&(tail + 1));

            // If the next transaction does not exist or is not consecutive, break
            // Unwrap ok because next_tail is not None
            if next_tail.is_none() || next_tail.unwrap().transaction_version != tail + 1 {
                break;
            }

            // Update tail and notify stream
            self.tail.store(tail + 1, Ordering::Relaxed);
            self.stream_notify.notify_waiters();
        }

        // Notify eviction task if cache size exceeds trigger size
        if self.cache.total_size() >= self.metadata.eviction_trigger_size_in_bytes {
            self.eviction_notify.notify_one();
        }
    }

    fn total_size(&self) -> usize {
        self.cache.total_size() as usize
    }
}

impl InMemoryCache {
    pub fn with_capacity(
        eviction_trigger_size_in_bytes: usize,
        target_size_in_bytes: usize,
        capacity: usize,
    ) -> Arc<Self> {
        let c = SyncMutexCache::with_capacity(capacity);
        let metadata = Arc::new(InMemoryCacheMetadata {
            eviction_trigger_size_in_bytes,
            target_size_in_bytes,
            capacity: c.capacity(),
        });
        let cache = Arc::new(c);
        let eviction_notify = Arc::new(Notify::new());

        let out = Arc::new(Self {
            metadata,
            cache,
            eviction_notify,
            stream_notify: Arc::new(Notify::new()),
            head: Arc::new(AtomicI64::new(-1)),
            tail: Arc::new(AtomicI64::new(-1)),
            watermark: Arc::new(AtomicI64::new(-1)),
        });

        spawn_eviction_task(out.clone());

        out
    }

    /// Returns a stream of values in the cache starting from the given (transaction_version, event_index).
    /// If the stream falls behind, the stream will return None for the next value (indicating that it should be reset).
    pub fn get_stream(
        &self,
        starting_key: Option<i64>,
    ) -> impl Stream<Item = Arc<CachedEvents>> + '_ {
        // Start from the starting key if provided, otherwise start from the last key
        let initial_state = starting_key.unwrap_or(self.tail.load(Ordering::Relaxed));

        Box::pin(stream::unfold(initial_state, move |state| {
            async move {
                // If the current key is None, the cache is empty
                // Wait until a new value is inserted before assigning it
                let mut current_transaction_version = state;
                if current_transaction_version == -1 {
                    self.eviction_notify.notified().await;
                    // Ok to unwrap because the last_transaction_version should be populated after the first insert
                    current_transaction_version = self.tail.load(Ordering::Relaxed);
                }

                let last_transaction_version = self.tail.load(Ordering::Relaxed);

                // Stream is ahead of cache
                // If the last value in the cache has already been streamed, wait until the next value is inserted and return it
                if current_transaction_version == last_transaction_version + 1 {
                    // Wait until the next value is inserted
                    loop {
                        self.stream_notify.notified().await;
                        if let Some(cached_events) = self.get(&current_transaction_version) {
                            return Some((cached_events, current_transaction_version + 1));
                        }
                    }
                }
                // Stream is in cache bounds
                // If the next value to stream is in the cache, return it
                else if let Some(cached_events) = self.get(&current_transaction_version) {
                    return Some((cached_events, current_transaction_version + 1));
                }

                // If we get here, the stream is behind the cache
                // Stop the stream
                None
            }
        }))
    }
}

/// Perform cache eviction on a separate task.
fn spawn_eviction_task(cache: Arc<InMemoryCache>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            cache.eviction_notify.notified().await;
            // Evict entries until the cache size is below the target size
            while cache.total_size() > cache.metadata.target_size_in_bytes {
                // Unwrap ok because eviction_notify is only notified after head is populated
                let eviction_key = cache.head.load(Ordering::Relaxed) as usize;
                if let Some(value) = cache.cache.evict(&eviction_key) {
                    if value.key > eviction_key {
                        cache.cache.insert_with_size(
                            value.key,
                            value.value.clone(),
                            value.size_in_bytes,
                        );
                        break;
                    }
                }

                // Update head
                cache.head.store(eviction_key as i64, Ordering::Relaxed);
            }
        }
    })
}
