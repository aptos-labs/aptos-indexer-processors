## Typescript / Node Quickstart

### Prerequisite

- `node`: The code is tested with Node 0.18.x. Later version should work too.

### Tutorial

1. Install the latest version of gRPC and tooling for Typescript:

```
npm install -g @grpc/grpc-js
npm install -g grpc-tools
```

2. Download the example:

```
# Clone the repository to get the example code:
$ git clone https://github.com/aptos-labs/aptos-indexer-processors.git

# Navigate to the typescript folder
$ cd aptos-indexer-processors/typescript

# Prepare the `config.yaml` file
$ cp config.yaml.example config.yaml
Make sure to update the `config.yaml` file with the correct indexer setting and database credentials.
```

In this example, we are creating an event parser.

3. Create a processer.
   - First you need to create an indexer processor that reads the stream of data.
   - We've create an example client in `processor.ts`. This client
     - Connects to the gRPC server and reads a stream of transaction data.
     - Calls the function `parse` to parse the transaction
     - Validates the chain ID and transaction version.
4. Create a parser.
   - In `event-parser.ts`, we have implemented a `parse` function which accepts a `Transaction` as a parameter.
   - The example code shows how to implement custom filtering and parse a `Transaction` and the associated `Event`'s.
   - The function returns a list of event objects, which we’ll need to add to the database..
5. Insert data rows into database.
   - In the example, we use Postgres and Typeorm to help us interact with the database.
   - In `processor.ts`, after the events are parsed, all the event objects are then added to the database.
6. Run ` npm run build && node build/processor.js process --config config.yaml` to start indexing!

### Install all dependencies

```bash
npm install
```
