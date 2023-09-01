import { aptos } from "@aptos-labs/aptos-indexer-protos";
import { Config } from "../../utils/config";
import { Timer } from "timer-node";
import { exit } from "process";
import { Metadata, credentials } from "@grpc/grpc-js";
import { parse as pgConnParse } from "pg-connection-string";
import { program } from "commander";
import { createDataSource } from "./models/data-source";
import { Event } from "./models/Event";
import { INDEXER_NAME, parse } from "./event-parser";
import { NextVersionToProcess } from "../../utils/common_models/NextVersionToProcess";

// https://github.com/grpc/grpc-node/issues/2565
/*
const originalCreateSsl = credentials.createSsl;
credentials.createSsl = function(...args) {
    const originalCreds = originalCreateSsl.apply(this, args);
    const settings: unknown = {
        // This increases the http2 frame size. The default is 16384, which is too small.
        maxFrameSize: 4194304,
        // The initial window size set here is overridden later. We need to patch the grpc-js
        // library to allow this.
        initialWindowSize: 4194304,
        maxHeaderListSize: 8192,
        enablePush: false,
      };
      // @ts-ignore
    originalCreds._getConnectionOptions = function() {
        return {
          settings,
        };
    };
    return originalCreds;
};
*/

program
  .command("process")
  .description("")
  .requiredOption("--config <config>", "Config file")
  .option(
    "--perf <number-transactions>",
    "Show perf metrics for processing <number-transactions>",
  )
  .action(async ({ config: conf, perf }) => {
    const config = Config.from_yaml_file(conf);

    // Initialize the database connection
    const options = pgConnParse(config.db_connection_uri);
    const port = options.port || "5432";
    if (!options.host || !options.database) {
      throw new Error(
        "Invalid postgres connection string. e.g. postgres://someuser:somepassword@somehost:5432/somedatabase",
      );
    }

    const dataSource = createDataSource(
      options.host!,
      Number(port),
      options.user,
      options.password,
      options.database,
      options.ssl as boolean,
    );

    await dataSource.initialize();

    // Create the grpc client
    const client = new aptos.indexer.v1.RawDataClient(
      config.grpc_data_stream_endpoint,
      credentials.createSsl(),
      {
        "grpc.keepalive_time_ms": 1000,
        // 0 - No compression
        // 1 - Compress with DEFLATE algorithm
        // 2 - Compress with GZIP algorithm
        // 3 - Stream compression with GZIP algorithm
        "grpc.default_compression_algorithm": 2,
        // 0 - No compression
        // 1 - Low compression level
        // 2 - Medium compression level
        // 3 - High compression level
        "grpc.default_compression_level": 3,
        // -1 means unlimited
        "grpc.max_receive_message_length": -1,
        // -1 means unlimited
        "grpc.max_send_message_length": -1,
      },
    );

    const startingVersion = BigInt(config.starting_version || 0n);

    const request: aptos.indexer.v1.GetTransactionsRequest = {
      startingVersion,
    };

    const metadata = new Metadata();
    metadata.set("Authorization", `Bearer ${config.grpc_data_stream_api_key}`);

    // Create and start the streaming RPC
    let currentTxnVersion = startingVersion;
    const stream = client.getTransactions(request, metadata);

    const timer = new Timer();
    timer.start();

    stream.on(
      "data",
      async function (response: aptos.indexer.v1.TransactionsResponse) {
        stream.pause();
        const transactionsList = response.transactions;

        if (transactionsList == null) {
          return;
        }

        console.log({
          message: "Response received",
          starting_version: transactionsList[0].version,
        });

        // Validate response chain ID matches expected chain ID
        if (response.chainId != config.chain_id) {
          throw new Error(
            `Chain ID mismatch. Expected ${config.chain_id} but got ${response.chainId}`,
          );
        }

        if (transactionsList == null) {
          return;
        }

        for (const transaction of transactionsList) {
          // Validate transaction version is correct
          if (transaction.version != currentTxnVersion) {
            throw new Error(
              `Transaction version mismatch. Expected ${currentTxnVersion} but got ${transaction.version}`,
            );
          }

          const parsedObjs = parse(transaction);
          if (parsedObjs.length > 0) {
            await dataSource.transaction(async (txnManager) => {
              await txnManager.insert(Event, parsedObjs);
              const nextVersionToProcess = createNextVersionToProcess(
                currentTxnVersion + 1n,
              );
              await txnManager.upsert(
                NextVersionToProcess,
                nextVersionToProcess,
                ["indexerName"],
              );
            });
          } else if (currentTxnVersion % 1000n === 0n) {
            // check point
            const nextVersionToProcess = createNextVersionToProcess(
              currentTxnVersion + 1n,
            );
            await dataSource
              .getRepository(NextVersionToProcess)
              .upsert(nextVersionToProcess, ["indexerName"]);

            console.log({
              message: "Successfully processed transaction",
              last_success_transaction_version: currentTxnVersion,
            });
          }

          const totalTransactions = currentTxnVersion - startingVersion + 1n;

          if (perf && totalTransactions >= Number(perf)) {
            timer.stop();
            console.log(
              `Takes ${timer.ms()} ms to receive ${totalTransactions} txns`,
            );
            exit(0);
          }

          currentTxnVersion += 1n;
        }
        stream.resume();
      },
    );

    stream.on("error", function (e) {
      console.error(e);
      // An error has occurred and the stream has been closed.
    });
    stream.on("status", function (status) {
      console.log(status);
      // process status
    });
  });

function createNextVersionToProcess(version: bigint) {
  const nextVersionToProcess = new NextVersionToProcess();
  nextVersionToProcess.nextVersion = version.toString();
  nextVersionToProcess.indexerName = INDEXER_NAME;
  return nextVersionToProcess;
}

program.parse();
