// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser;
use processor::{from_v1_config, IndexerGrpcProcessorConfig, IndexerGrpcProcessorConfigV2};
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
            match args.load_generic_config::<IndexerGrpcProcessorConfigV2>() {
                Ok(_) => {
                    args.run::<IndexerGrpcProcessorConfigV2>(tokio::runtime::Handle::current())
                        .await
                },
                Err(_) => {
                    let v1_config = args.load_generic_config::<IndexerGrpcProcessorConfig>()?;
                    let v2_config = from_v1_config(v1_config);
                    println!(
                        "You are using a deprecated config format, please migrate! The equivalent config in new format is: {}",
                        serde_yaml::to_string(&v2_config).unwrap()
                    );
                    args.pre_run();
                    server_framework::run_server_with_config(
                        v2_config,
                        tokio::runtime::Handle::current(),
                    )
                    .await
                },
            }
        })
}
