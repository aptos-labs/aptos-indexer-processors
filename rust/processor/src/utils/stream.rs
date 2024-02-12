// Copyright © Aptos Foundation

use crate::models::events_models::events::EventModel;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use std::time::Duration;
use tokio::{
    sync::broadcast::{Receiver, Sender},
    time,
};
use tracing::{error, info};
use warp::filters::ws::{Message, WebSocket};

pub struct Stream {
    tx: SplitSink<WebSocket, Message>,
    channel: Receiver<EventModel>,
    websocket_alive_duration: u64,
}

impl Stream {
    pub fn new(
        tx: SplitSink<WebSocket, Message>,
        channel: Receiver<EventModel>,
        websocket_alive_duration: u64,
    ) -> Self {
        info!("Received WebSocket connection");
        Self {
            tx,
            channel,
            websocket_alive_duration,
        }
    }

    /// Maintains websocket connection and sends messages from channel
    pub async fn run(&mut self) {
        let sleep = time::sleep(Duration::from_secs(self.websocket_alive_duration));
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                msg = self.channel.recv() => {
                    let event = msg.unwrap_or_else(|e| {
                        error!(
                            error = ?e,
                            "[Event Stream] Failed to receive message from channel"
                        );
                        panic!();
                    });
                    self.tx
                        .send(warp::ws::Message::text(serde_json::to_string(&event).unwrap()))
                        .await
                        .unwrap();
                },
                _ = &mut sleep => {
                    break;
                }
            }
        }
    }
}

pub async fn spawn_stream(
    ws: WebSocket,
    channel: Sender<EventModel>,
    websocket_alive_duration: u64,
) {
    let (tx, _) = ws.split();
    let mut stream = Stream::new(tx, channel.subscribe(), websocket_alive_duration);
    stream.run().await;
}
