use anyhow::Result;
use aptos_indexer_processor_example::config::indexer_processor_config::IndexerProcessorConfig;
use aptos_indexer_processor_sdk_server_framework::ServerArgs;
use clap::Parser;

#[cfg(unix)]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus).max(16);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async {
            let args = ServerArgs::parse();
            args.run::<IndexerProcessorConfig>(tokio::runtime::Handle::current())
                .await
        })
}
