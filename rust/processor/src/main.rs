// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser;
use processor::IndexerGrpcProcessorConfig;
use server_framework::ServerArgs;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);
    println!(
        "[Processor] Starting processor tokio runtime: num_cpus={}, worker_threads={}",
        num_cpus, worker_threads
    );

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async {
            let args = ServerArgs::parse();
            args.run::<IndexerGrpcProcessorConfig>(tokio::runtime::Handle::current())
                .await
        })
}
