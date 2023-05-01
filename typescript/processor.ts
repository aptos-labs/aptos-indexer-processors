import { ArgumentParser } from "argparse";
import { parse } from "./grpc_parser";
import * as services from "./aptos/indexer/v1/raw_data_grpc_pb";
import * as indexerRawDataMessages from "./aptos/indexer/v1/raw_data_pb";
import * as transactionMessages from "./aptos/transaction/testing1/v1/transaction_pb";
import { Config } from "./config";
import * as grpc from "@grpc/grpc-js";

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
const client = new services.RawDataClient(
  config.indexer_endpoint,
  grpc.credentials.createInsecure(),
  {
    "grpc.default_compression_algorithm": 2,
    "grpc.default_compression_level": 3,
  }
);

const request = new indexerRawDataMessages.GetTransactionsRequest();
request.setStartingVersion(config.starting_version);
const metadata = new grpc.Metadata();
metadata.set("x-aptos-data-authorization", config.indexer_api_key);

// Create and start the streaming RPC
let currentTransactionVersion = config.starting_version;
const stream = client.getTransactions(request, metadata);

stream.on(
  "data",
  function (response: indexerRawDataMessages.TransactionsResponse) {
    const transactionsList = response.getTransactionsList();

    if (transactionsList == null) {
      return;
    }

    // Validate response chain ID matches expected chain ID
    if (response.getChainId() != config.chain_id) {
      throw new Error(
        "Chain ID mismatch. Expected " +
          config.chain_id +
          " but got " +
          response.getChainId()
      );
    }

    if (transactionsList == null) {
      return;
    }

    for (const transaction of transactionsList) {
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

      if (currentTransactionVersion % 1000 == 0) {
        console.log({
          message: "Successfully processed transaction",
          last_success_transaction_version: currentTransactionVersion,
        });
      }

      currentTransactionVersion += 1;
    }
  }
);

stream.on("error", function (e) {
  // An error has occurred and the stream has been closed.
});
stream.on("status", function (status) {
  // process status
});
