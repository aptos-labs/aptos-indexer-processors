// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

mod grpc;

use aptos_indexer_protos::transaction::v1::Transaction;
pub use grpc::{GrpcStreamSubscriber, GrpcStreamSubscriberConfig};
use thiserror::Error;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

#[async_trait::async_trait]
pub trait StreamSubscriberTrait: 'static + Send + Sync {
    async fn start(&self) -> Result<ChannelHandle, StartError>;
}

pub struct ChannelHandle {
    pub join_handle: JoinHandle<()>,
    pub receiver: Receiver<(u8, Vec<Transaction>)>,
}

#[derive(Error, Debug)]
pub enum StartError {
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Failed to connect: {0}")]
    FailedToConnect(String),
}
