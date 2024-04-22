# Aptos Indexer Client Guide
This guide will get you started with creating an Aptos indexer with custom parsing. We have several endpoints that provided a streaming RPC of transaction data.

## GRPC Data Stream Endpoints
* devnet: https://grpc.devnet.aptoslabs.com:443

* testnet: https://grpc.testnet.aptoslabs.com:443

* mainnet: https://grpc.mainnet.aptoslabs.com:443

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
- To learn more about the protos and the code generated from those protos see [protos/](https://github.com/aptos-labs/aptos-core/tree/main/protos) in aptos-core.

## [Aptos Indexer GRPC Release Notes](https://github.com/aptos-labs/aptos-core/blob/main/ecosystem/indexer-grpc/release_notes.md)


> [!WARNING]  
> The typescript implementation is known to get stuck when there are lots of data to process. The issue is with the GRPC client and we haven't had a chance to optimize. Please proceed with caution.