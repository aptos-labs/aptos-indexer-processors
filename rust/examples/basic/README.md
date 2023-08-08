# Basic
This example code demonstrates how you can use on the processor framework code to write your own processor. It uses dummy processor and storage implementations for the sake of the demonstration.

## Running
The config included in this repo assumes you are running a local testnet and txn stream service. You can do both with this script:
```
cd ~/aptos-core
cd testsuite
poetry run python indexer_grpc_local.py start
```

If you do that, the next step should work without any further changes. 
```
cargo run -- --config-path config.yaml
```

If you'd like to connect to a production network's txn stream service instead you need to change the `indexer_grpc_data_service_address` and `auth_token` config values in `config.yaml`.

