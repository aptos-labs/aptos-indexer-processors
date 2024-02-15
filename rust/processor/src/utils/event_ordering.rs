use crate::models::events_models::events::EventModel;
use kanal::AsyncReceiver;
use std::{
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};
use tokio::sync::{broadcast::Sender, Mutex};
use tracing::{error, warn};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionEvents {
    pub transaction_version: i64,
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

pub struct EventOrdering {
    rx: Arc<AsyncReceiver<TransactionEvents>>,
    tx: Sender<EventModel>,
}

impl EventOrdering {
    pub fn new(rx: AsyncReceiver<TransactionEvents>, tx: Sender<EventModel>) -> Self {
        Self {
            rx: Arc::new(rx),
            tx,
        }
    }

    pub async fn run(&mut self, starting_version: i64) {
        let heap_arc_mutex = Arc::new(Mutex::new(BinaryHeap::new()));
        let rx = self.rx.clone();

        let heap_push = heap_arc_mutex.clone();
        let push_thread = tokio::spawn(async move {
            loop {
                // let heap = heap_push.clone();
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

        let tx = self.tx.clone();
        let heap_pop = heap_arc_mutex.clone();
        let next_transaction_version = AtomicI64::new(starting_version);
        let pop_thread = tokio::spawn(async move {
            loop {
                let mut heap_locked = heap_pop.lock().await;
                if let Some(transaction_events) = heap_locked.peek() {
                    if transaction_events.transaction_version
                        == next_transaction_version.load(Ordering::SeqCst)
                    {
                        let transaction_events = heap_locked.pop().unwrap_or_else(|| {
                            error!("Failed to pop events from the heap");
                            panic!();
                        });
                        for event in transaction_events.events {
                            tx.send(event).unwrap_or_else(|e| {
                                warn!(
                                    error = ?e,
                                    "Failed to send event to the channel"
                                );
                                0
                            });
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
