// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser;
use processor::IndexerGrpcProcessorConfig;
use server_framework::ServerArgs;
use std::sync::atomic::{AtomicUsize, Ordering};

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);
    println!(
        "[Processor] Starting processor tokio runtime: num_cpus={}, worker_threads={}",
        num_cpus, worker_threads
    );

    let atomic_id = AtomicUsize::new(0);

    tokio::runtime::Builder::new_multi_thread()
        .thread_name_fn(move || {
            let id = atomic_id.fetch_add(1, Ordering::SeqCst);
            format!("tokio-{}", id)
        })
        .enable_all()
        .worker_threads(worker_threads)
        .disable_lifo_slot()
        .build()
        .unwrap()
        .block_on(async {
            let args = ServerArgs::parse();
            args.run::<IndexerGrpcProcessorConfig>(tokio::runtime::Handle::current())
                .await
        })
}
