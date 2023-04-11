# Aptos Indexer Client Guide
This guide will get you started with creating an Aptos indexer with custom parsing. We have several endpoints that provided a streaming RPC of transaction data. 

## Indexer Endpoints
devent: 34.70.26.67:50051

testnet: 35.223.137.149:50051

previewnet: 104.154.118.201:50051

mainnet: 34.30.218.153:50051

## Request
 - When making a request to the indexer, setting the transaction version `starting_version` is required. In the example code, we use `starting_version=10000`. You can update this with `starting_version=0` to start from genesis or the next transaction version you want to index. 
## Response
- The response is guaranteed to return a stream of sequential transactions. Keep track of the latest transaction version your indexer has already processed, so you can easily restart your indexer and continue from the latest transaction version by setting `starting_version`. 

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
The folder `aptos-indexer-client-examples/python/aptos` contains all the auto-generated protobuf Python code. You can check out the `.pyi` files to see the stream response format and how to parse the response.

3. Update `grpc_client.py`.
   - To connect to an indexer endpoint, update `("x-aptos-data-authorization", "YOUR_TOKEN")` with your auth token.
   - The example code uses `grpc.insecure_channel(MAINNET_INDEXER_ENDPOINT, options=options)`. You can update this to read from devnet, testnet, or previewnet.
  
4. Update `grpc_parser.py`
   - In `grpc_parser.py`, we have implemented a `parse` function which accepts a `Transaction` as a parameter.
   - The example code shows how to implement custom filtering and parse a `Transaction` and the associated `Event`'s.
   - Implement the `insert_into_db` function to insert the data into your DB of choice. 
5. Run `python grpc_client.py` to start indexing! 

## Typescript 
## Rust
