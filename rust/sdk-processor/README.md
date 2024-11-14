# Transaction Stream Processors (SDK)
Processors that index data from the Aptos Transaction Stream (GRPC). These processors have been (re)-written using the new Indexer SDK.

- **Note: Official releases coming soon!**

## Tutorial
Follow tutorial in the [example repo](https://github.com/aptos-labs/aptos-indexer-processor-example).

## Processor Specific Notes

### Supported Coin Type Mappings
See mapping in [v2_fungible_asset_balances.rs](https://github.com/aptos-labs/aptos-indexer-processors/blob/main/rust/processor/src/db/common/models/fungible_asset_models/v2_fungible_asset_balances.rs#L40) for a list supported coin type mappings.