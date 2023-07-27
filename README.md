# Aptos Indexer Client Guide
This guide will get you started with creating an Aptos indexer with custom parsing. We have several endpoints that provided a streaming RPC of transaction data. 

## GRPC Data Stream Endpoints(all endpoints are in GCP us-central1 unless specified)
* devnet: 35.225.218.95:50051

* testnet: 35.223.137.149:50051
  * Asia(GCP asia-northeast3): 34.64.252.224:50051

* mainnet: 34.30.218.153:50051

## Request
 - `config.yaml`
   - `chain_id`: ID of the chain used for validation purposes. 
   - `grpc_data_stream_endpoint`: Replace with the grpc data stream endpoints for mainnet, devnet, testnet, or previewnet. 
   - `grpc_data_stream_api_key`: Replace `YOUR_TOKEN` with your auth token.
   - `db_connection_uri`: The DB connection used to write the processed data 
   - (optional) `starting-version`
     - If `starting-version` is set, the processor will begin indexing from transaction version = `starting_version`.
     - To auto restart the client in case of an error, you can cache the latest processed transaction version. In the example, the processor restarts from cached transaction version that is stored in a table, and if neither `starting_version` nor cached version are set, the processor defaults starting version to 0. 

## Response
- The response is a stream of `RawDatastreamResponse` objects.
- For each supported language, there is an `aptos` folder which contains the auto-generate protobuf files in that language. You can check out the files to see the stream response format and figure out how to parse the response. 

## [Aptos Indexer GRPC Release Notes](https://github.com/aptos-labs/aptos-core/blob/main/ecosystem/indexer-grpc/release_notes.md)


## Dev Guide

### Installation

```bash
# Install buf
brew install bufbuild/buf/buf

# For Rust generated code
cargo install protoc-gen-prost
cargo install protoc-gen-prost-serde
cargo install protoc-gen-prost-crate
cargo install protoc-gen-tonic

# For TS generated code
pnpm install -g protoc-gen-ts google-protobuf typescript@4.x.x 

```

### Development

```
./build_protos.sh
python3 -m grpc_tools.protoc --proto_path=./proto --python_out=python --pyi_out=python --grpc_python_out=python \
proto/aptos/bigquery_schema/v1/transaction.proto \
proto/aptos/indexer/v1/raw_data.proto \
proto/aptos/internal/fullnode/v1/fullnode_data.proto \
proto/aptos/transaction/v1/transaction.proto \
proto/aptos/util/timestamp/timestamp.proto

```
at top-level to generate the proto code.
