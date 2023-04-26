import { ArgumentParser } from "argparse";
import { Config } from "./config";
import * as messages from "./aptos/datastream/v1/datastream_pb";
import * as grpc from "@grpc/grpc-js";
import * as services from "./aptos/datastream/v1/datastream_grpc_pb";

// Parse the config file
const parser = new ArgumentParser({
  description: "Indexer Stream Client arguments",
});
parser.add_argument("-c", "--config", {
  help: "Path to config file",
  required: true,
});
const args = parser.parse_args();
const config = Config.from_yaml_file(args.config);

// Create client and request
const client = new services.IndexerStreamClient(
  config.indexer_endpoint,
  grpc.credentials.createInsecure(),
  {
    "grpc.default_compression_algorithm": 2,
    "grpc.default_compression_level": 3,
  }
);

const request = new messages.RawDatastreamRequest();
request.setStartingVersion(config.starting_version);
const metadata = new grpc.Metadata();
metadata.set("x-aptos-data-authorization", config.indexer_api_key);

// Create and start the streaming RPC
const stream = client.rawDatastream(request, metadata);

let start = performance.now();
let total = 0;
let count = 0;
stream.on(
  "data",
  function (response: datastreamMessages.RawDatastreamResponse) {
    const transactionsOutput = response.getData();

    if (transactionsOutput == null) {
      return;
    }

    console.log("transaction version", currentTransactionVersion);
    const time = performance.now();
    const diff = time - start;
    total += diff;
    count += 1;

    // Validate response chain ID matches expected chain ID
    if (response.getChainId() != config.chain_id) {
      throw new Error(
        "Chain ID mismatch. Expected " +
          config.chain_id +
          " but got " +
          response.getChainId()
      );
    }

    if (transactionsOutput == null) {
      return;
    }

    for (const transactionOutput of transactionsOutput.getTransactionsList()) {
      // Decode transaction object
      const serializedTransactionString = Base64.decode(
        transactionOutput.getEncodedProtoData_asB64()
      );
      const serializedTransactionBytes = Base64.toUint8Array(
        serializedTransactionString
      );
      const transaction = transactionMessages.Transaction.deserializeBinary(
        serializedTransactionBytes
      );

      // Validate transaction version is correct
      if (transaction.getVersion() != currentTransactionVersion) {
        throw new Error(
          "Transaction version mismatch. Expected " +
            currentTransactionVersion +
            " but got " +
            transaction.getVersion()
        );
      }

      parse(transaction);

      currentTransactionVersion += 1;
    }

    start = performance.now();
  }
);

stream.on("error", function (e) {
  // An error has occurred and the stream has been closed.
});
stream.on("status", function (status) {
  // process status
});

process.on("SIGINT", function () {
  console.log("Average time:", total / count);
  process.exit();
});
