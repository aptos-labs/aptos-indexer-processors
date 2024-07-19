use anyhow::Result;
use clap::Parser;
use sdk_examples::config::indexer_processor_config::IndexerProcessorConfig;
use sdk_server_framework::ServerArgs;

#[cfg(unix)]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);

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
