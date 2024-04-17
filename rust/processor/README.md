# Indexer GRPC Parser

Indexer GRPC parser is to indexer data processor that leverages the indexer grpc data.

- **Note: We'll launch an official endpoint soon; stay tuned!**

## Tutorial

### Prerequisite

- A running PostgreSQL instance, with a valid database. More tutorial can be
  found [here](https://github.com/aptos-labs/aptos-core/tree/main/crates/indexer#postgres)

- A config YAML file
    - For example, `config.yaml`

      ```yaml
      health_check_port: 8084
      server_config:
        processor_config:
          type: default_processor
        postgres_connection_string: postgresql://postgres:@localhost:5432/postgres_v2
        indexer_grpc_data_service_address: 127.0.0.1:50051
        indexer_grpc_http2_ping_interval_in_secs: 60
        indexer_grpc_http2_ping_timeout_in_secs: 10
        number_concurrent_processing_tasks: 10
        auth_token: AUTH_TOKEN
        starting_version: 0 # optional
        ending_version: 0 # optional
        transaction_filter:
          # Only allow transactions from these contract addresses
          # focus_contract_addresses:
          #   - "0x0"
          # Skip transactions from these sender addresses
          skip_sender_addresses:
            - "0x07"
          # Skip all transactions that aren't user transactions
          focus_user_transactions: false
      ```

#### Config Explanation

- `type` in `processor_config`: purpose of this processor; also used for monitoring purpose.
- `postgres_connection_string`: PostgresQL DB connection string
- `indexer_grpc_data_service_address`: Data service non-TLS endpoint address.
- `indexer_grpc_http2_ping_interval_in_secs`: client-side grpc HTTP2 ping interval.
- `indexer_grpc_http2_ping_timeout_in_secs`: client-side grpc HTTP2 ping timeout.
- `auth_token`: Auth token used for connection.
- `starting_version`: start processor at starting_version.
- `ending_version`: stop processor after ending_version.
- `number_concurrent_processing_tasks`: number of tasks to parse and insert; 1 means sequential processing, otherwise,
  transactions are splitted into tasks and inserted with random order.

### Use docker image for existing parsers(Only for **Unix/Linux**)

- Use the provided `Dockerfile` and `config.yaml`(update accordingly)
    - Build: `cd ecosystem/indexer-grpc/indexer-grpc-parser && docker build . -t indexer-processor`
    - Run: `docker run indexer-processor:latest`

### Use source code for existing parsers

- Use the provided `Dockerfile` and `config.yaml`(update accordingly)
- Run `cd rust/processor && cargo run --release -- -c config.yaml`

### Use a custom parser

- Check our [indexer processors](https://github.com/aptos-labs/aptos-indexer-processors)!

### Manually running diesel-cli
- `cd` into the database folder under `src/db/`, then run it.
