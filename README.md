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
   - `indexer_endpoint`: Replace with the indexer endpoints for mainnet, devnet, testnet, or previewnet. 
   - `indexer_api_key`: Replace `YOUR_TOKEN` with your auth token.
   - `db_connection_uri`: The DB connection used to write the processed data 
   - (optional) `starting-version`
     - If `starting-version` is set, the processor will begin indexing from transaction version = `starting_version`.
     - To auto restart the client in case of an error, you can cache the latest processed transaction version. In the example, the processor restarts from cached transaction version that is stored in a table, and if neither `starting_version` nor cached version are set, the processor defaults starting version to 0. 

## Response
- The response is a stream of `RawDatastreamResponse` objects.
- For each supported language, there is an `aptos` folder which contains the auto-generate protobuf files in that language. You can check out the files to see the stream response format and figure out how to parse the response. 

## [Aptos Indexer GRPC Release Notes](https://github.com/aptos-labs/aptos-core/blob/main/ecosystem/indexer-grpc/release_notes.md)
