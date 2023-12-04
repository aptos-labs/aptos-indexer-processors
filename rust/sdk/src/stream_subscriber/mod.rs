// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

mod grpc;

use aptos_protos::transaction::v1::Transaction;
pub use grpc::{GrpcStreamSubscriber, GrpcStreamSubscriberConfig, IndexerGrpcHttp2Config};
use thiserror::Error;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

#[async_trait::async_trait]
pub trait StreamSubscriberTrait: 'static + Send + Sync {
    fn start(&self) -> Result<ChannelHandle, StartError>;
}

#[derive(Clone)]
pub struct TransactionsPBResponse {
    pub transactions: Vec<Transaction>,
    pub chain_id: u64,
    pub size_in_bytes: u64,
}

pub struct ChannelHandle {
    pub join_handle: JoinHandle<()>,
    pub receiver: Receiver<TransactionsPBResponse>,
}

#[derive(Error, Debug)]
pub enum StartError {
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Failed to connect: {0}")]
    FailedToConnect(String),
}
