# Aptos Core Processors (SDK version)
Processors that index data from the Aptos Transaction Stream (GRPC). These processors have been (re)-written using the new Indexer SDK.

- **Note: Official releases coming soon!**

## Overview
This tutorial shows you how to run the Aptos core processors in this repo.

If you want to index a custom contract, we recommend using the [Quickstart Guide](https://aptos.dev/en/build/indexer/indexer-sdk/quickstart).

### Prerequisite

- A running PostgreSQL instance, with a valid database. More tutorial can be
  found [here](https://github.com/aptos-labs/aptos-core/tree/main/crates/indexer#postgres)

- [diesel-cli](https://diesel.rs/guides/getting-started)

- A `config.yaml` file
    ```yaml
    # This is a template yaml for the sdk-processor.
    health_check_port: 8085
    server_config:
        processor_config:
            type: "fungible_asset_processor"
            channel_size: 100
        transaction_stream_config:
            indexer_grpc_data_service_address: "https://grpc.mainnet.aptoslabs.com:443"
            starting_version: 0
            # request_ending_version: 1409805
            auth_token: "{AUTH_TOKEN}"
            request_name_header: "fungible_asset_processor"
        db_config:
            type: "postgres_config"
            connection_string: postgresql://postgres:@localhost:5432/example
    ```

#### `config.yaml` Explanation

- `processor_config`
    - `type`: which processor to run
    - `channel_size`: size of channel in between steps
    - Individual processors may have different configuration required. See the full list of configs [here](https://github.com/aptos-labs/aptos-indexer-processors/blob/main/rust/sdk-processor/src/config/processor_config.rs#L89).
- `transaction_stream_config`
    - `indexer_grpc_data_service_address`: Data service non-TLS endpoint address.
    - `auth_token`: Auth token used for connection.
    - `starting_version`: start processor at starting_version.
    - `request_ending_version`: stop processor after ending_version.
    - `request_name_header`: request name header to append to the grpc request; name of the processor
    - `additional_headers`: addtional headers to append to the grpc request
    - `indexer_grpc_http2_ping_interval_in_secs`: client-side grpc HTTP2 ping interval.
    - `indexer_grpc_http2_ping_timeout_in_secs`: client-side grpc HTTP2 ping timeout.
    - `indexer_grpc_reconnection_timeout_secs`: grpc reconnection timeout
    - `indexer_grpc_response_item_timeout_secs`: grpc response item timeout
   
- `db_config`
    - `type`: type of storage, `postgres_config` or `parquet_config`
    - `connection_string`: PostgresQL DB connection string


### Use docker image for existing processors (Only for **Unix/Linux**)

- Use the provided `Dockerfile` and `config.yaml` (update accordingly)
    - Build: `cd ecosystem/indexer-grpc/indexer-grpc-parser && docker build . -t indexer-processor`
    - Run: `docker run indexer-processor:latest`

### Use source code for existing parsers

- Use the provided `Dockerfile` and `config.yaml` (update accordingly)
- Run `cd rust/sdk-processor && cargo run --release -- -c config.yaml`


### Manually running diesel-cli
- `cd` into the database folder you use under `rust/processor/src/db/` (e.g. `rust/processor/src/db/postgres`), then run it.

## Processor Specific Notes

### Supported Coin Type Mappings
See mapping in [v2_fungible_asset_balances.rs](https://github.com/aptos-labs/aptos-indexer-processors/blob/main/rust/processor/src/db/common/models/fungible_asset_models/v2_fungible_asset_balances.rs#L40) for a list supported coin type mappings.