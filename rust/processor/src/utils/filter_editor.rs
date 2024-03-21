// Copyright © Aptos Foundation

use crate::utils::filter::EventFilter;
use futures::{stream::SplitStream, StreamExt};
use std::sync::Arc;
use tracing::{error, info};
use warp::filters::ws::WebSocket;

pub struct FilterEditor {
    rx: SplitStream<WebSocket>,
    filter: Arc<EventFilter>,
}

impl FilterEditor {
    pub fn new(rx: SplitStream<WebSocket>, filter: Arc<EventFilter>) -> Self {
        info!("Received WebSocket connection");
        Self { rx, filter }
    }

    /// Maintains websocket connection and sends messages from channel
    pub async fn run(&mut self) {
        while let Some(Ok(msg)) = self.rx.next().await {
            if let Ok(policy) = msg.to_str() {
                let policy = policy.split(",").collect::<Vec<&str>>();
                match policy[0] {
                    "account" => match policy[1] {
                        "add" => {
                            self.filter.accounts.insert(policy[2].to_string());
                        },
                        "remove" => {
                            self.filter.accounts.remove(policy[2]);
                        },
                        _ => {
                            error!("[Event Stream] Invalid filter command: {}", policy[1]);
                        },
                    },
                    "type" => match policy[1] {
                        "add" => {
                            self.filter.types.insert(policy[2].to_string());
                        },
                        "remove" => {
                            self.filter.types.remove(policy[2]);
                        },
                        _ => {
                            error!("[Event Stream] Invalid filter command: {}", policy[1]);
                        },
                    },
                    _ => {
                        error!("[Event Stream] Invalid filter type: {}", policy[0]);
                    },
                }
            }
        }
    }
}

pub async fn spawn_filter_editor(rx: SplitStream<WebSocket>, filter: Arc<EventFilter>) {
    let mut filter = FilterEditor::new(rx, filter);
    filter.run().await;
}
