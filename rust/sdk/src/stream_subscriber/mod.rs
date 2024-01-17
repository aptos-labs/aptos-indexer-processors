// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

mod grpc;

use anyhow::Result;
use aptos_protos::transaction::v1::Transaction;
pub use grpc::{GrpcStreamSubscriber, GrpcStreamSubscriberConfig, IndexerGrpcHttp2Config};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

/// This trait defines something that subscribes to a stream of transactions, puts them
/// in a channel, and returns the receiver end of that channel. We expect this to also
/// return a JoinHandle that the user can use to check that the StreamSubscriber is
/// still working, cancel it, etc.
#[async_trait::async_trait]
pub trait StreamSubscriberTrait: 'static + Send + Sync {
    fn start(&self) -> Result<ChannelHandle>;
}

#[derive(Clone)]
pub struct TransactionsPBResponse {
    /// The transactions that were returned by the server, in version order.
    pub transactions: Vec<Transaction>,

    /// The chain ID corresponding to the network the upstream is serving.
    pub chain_id: u8,

    /// The size in bytes of this chunk of transactions returned by the server.
    pub size_in_bytes: u64,
}

pub struct ChannelHandle {
    /// The JoinHandle for the inner StreamSubscriber task.
    pub join_handle: JoinHandle<()>,

    /// The receiver end of the channel that the StreamSubscriber is putting
    /// transactions into.
    pub receiver: Receiver<TransactionsPBResponse>,
}
