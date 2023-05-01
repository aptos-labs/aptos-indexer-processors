# Aptos Indexer Client Guide
This guide will get you started with creating an Aptos indexer with custom parsing. We have several endpoints that provided a streaming RPC of transaction data. 

## Indexer Endpoints
devent: 34.70.26.67:50051

testnet: 35.223.137.149:50051

previewnet: 104.154.118.201:50051

mainnet: 34.30.218.153:50051

## Request
 - `config.yaml`
   - `chain_id`: ID of the chain used for validation purposes. 
   - `indexer_endpoint`: You can replace this with the endpoints for devnet, testnet, or previewnet. 
   - `x-aptos-data-authorization`: Replace `YOUR_TOKEN` with your auth token.
   - `starting-version`
     - When making a request to the indexer, setting the transaction version `starting_version` is required. In the example code, we use `starting-version=10000`. You can update this with `starting_version=0` to start from genesis or the next transaction version you want to index. 
     - If you want to auto restart the client in case of an error, you should cache the latest processed transaction version, and start the next run with the transaction version from cache instead of manually specifying it in `config.yaml`.
## Response
- The response is a stream of `RawDatastreamResponse` objects.
- For each supported language, there is an `aptos` folder which contains the auto-generate protobuf files in that language. You can check out the files to see the stream response format and figure out how to parse the response. 

## Typescript / Node
### Prerequisite
- `node`: This requires Node 0.12.x or greater.
### Guide
1. Install the latest version of gRPC and tooling for Typescript:
  ```
  npm install -g @grpc/grpc-js
  npm install -g grpc-tools
  ```
2. 
## Rust
