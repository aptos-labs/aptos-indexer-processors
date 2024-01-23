// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser;
use processor::IndexerGrpcProcessorConfig;
use server_framework::ServerArgs;
use tracing::info;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).min(16);
    info!(
        num_cpus = num_cpus,
        num_worker_threads = worker_threads,
        "[Processor] Starting processor tokio runtime",
    );

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads((num_cpus::get() * RUNTIME_WORKER_MULTIPLIER).min(16))
        .build()
        .unwrap()
        .block_on(async {
            let args = ServerArgs::parse();
            args.run::<IndexerGrpcProcessorConfig>(tokio::runtime::Handle::current())
                .await
        })
}
