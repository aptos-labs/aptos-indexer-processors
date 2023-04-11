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

# Quickstart
## Python
1. Follow the quickstart guide to install gRPC and tooling for Python
```
python -m pip install grpcio
python -m pip install grpcio-tools
```
2. Download the example
```
# Clone the repository to get the example code:
$ git clone https://github.com/aptos-labs/aptos-indexer-client-examples
# Navigate to the python folder
$ cd aptos-indexer-client-examples/python
```
3. Update `grpc_parser.py`
   - In `grpc_parser.py`, we have implemented a `parse` function which accepts a `Transaction` as a parameter.
   - The example code shows how to implement custom filtering and parse a `Transaction` and the associated `Event`'s.
   - Implement the `insert_into_db` function to insert the data into your DB of choice. 
4. Run `python grpc_client.py` to start indexing! 

## Typescript 
## Rust
