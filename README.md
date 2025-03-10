> [!IMPORTANT]
> 
> Latest development is no longer done in this repo.
> 
> - [aptos-indexer-processors-v2](https://github.com/aptos-labs/aptos-indexer-processors-v2) for processors
> - [aptos-indexer-processor-sdk](https://github.com/aptos-labs/aptos-indexer-processor-sdk) for processor-sdk
> - [aptos-indexer-processor-example](https://github.com/aptos-labs/aptos-indexer-processor-example) for processor sdk quickstart


[![codecov](https://codecov.io/gh/aptos-labs/aptos-indexer-processors/graph/badge.svg?token=yOKOnndthm)](https://codecov.io/gh/aptos-labs/aptos-indexer-processors)


# Aptos Core Processors
These are the core processors that index data for the [Indexer API](https://aptos.dev/en/build/indexer/aptos-hosted). 
The core processors live in the `sdk-processor` crate and are written in Rust using the [Indexer SDK](https://aptos.dev/en/build/indexer/indexer-sdk).
Read more about indexing on Aptos [here](https://aptos.dev/en/build/indexer).
If you want to create a custom processor to index your contract, start with our [Quickstart Guide](https://aptos.dev/en/build/indexer/indexer-sdk/quickstart).

> [!WARNING]  
> For production-grade indexers, we recommend using the [Indexer SDK](https://aptos.dev/en/build/indexer/indexer-sdk) to write your custom processor.
> The Python implementation is known to have a grpc deserialization recursion limit. The issue is with the GRPC library and we haven't had a chance to look into this. Please proceed with caution.
> The typescript implementation is known to get stuck when there are lots of data to process. The issue is with the GRPC client and we haven't had a chance to optimize. Please proceed with caution.
