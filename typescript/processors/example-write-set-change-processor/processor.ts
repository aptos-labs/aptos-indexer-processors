import * as services from "@/aptos/indexer/v1/raw_data_grpc_pb";
import {
  GetTransactionsRequest,
  TransactionsResponse,
} from "@/aptos/indexer/v1/raw_data_pb";
import { Config } from "@/utils/config";
import { Timer } from "timer-node";
import { exit } from "process";
import { ChannelCredentials, CallCredentials, Metadata } from "@grpc/grpc-js";
import { parse as pgConnParse } from "pg-connection-string";
import { program } from "commander";
import { createDataSource } from "@/processors/example-write-set-change-processor/models/data-source";
import { Event } from "@/processors/example-write-set-change-processor/models/Event";
import {
  INDEXER_NAME,
  parse,
} from "@/processors/example-write-set-change-processor/event-parser";
import { NextVersionToProcess } from "@/utils/common_models/NextVersionToProcess";

// A hack to override the http2 settings
class CustomChannelCred extends ChannelCredentials {
  constructor(callCredentials?: CallCredentials) {
    super();
  }

  compose(callCredentials: CallCredentials): never {
    throw new Error("Cannot compose insecure credentials");
  }

  _getConnectionOptions(): any {
    return {
      settings: {
        // This will increase the http2 frame size. Default is 16384, which is too small.
        maxFrameSize: 4194304,
        // The initial window size set here is overridden later. We need to patch the grpc-js library to allow this.
        initialWindowSize: 4194304,
        maxHeaderListSize: 8192,
        enablePush: false,
        maxConcurrentStreams: 0,
      },
    };
  }

  _isSecure(): boolean {
    return false;
  }

  _equals(other: ChannelCredentials): boolean {
    return other instanceof CustomChannelCred;
  }
}

program
  .command("process")
  .description("")
  .requiredOption("--config <config>", "Config file")
  .option(
    "--perf <number-transactions>",
    "Show perf metrics for processing <number-transactions>"
  )
  .action(async ({ config: conf, perf }) => {
    const config = Config.from_yaml_file(conf);

    // Initialize the database connection
    const options = pgConnParse(config.db_connection_uri);
    const port = options.port || "5432";
    if (!options.host || !options.database) {
      throw new Error(
        "Invalid postgres connection string. e.g. postgres://someuser:somepassword@somehost:5432/somedatabase"
      );
    }

    const dataSource = createDataSource(
      options.host!,
      Number(port),
      options.user,
      options.password,
      options.database,
      options.ssl as boolean
    );

    await dataSource.initialize();

    // Create the grpc client
    const client = new services.RawDataClient(
      config.grpc_data_stream_endpoint,
      new CustomChannelCred(),
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
      }
    );

    const request = new GetTransactionsRequest();
    request.setStartingVersion(config.starting_version.toString());
    const metadata = new Metadata();
    metadata.set("x-aptos-data-authorization", config.grpc_data_stream_api_key);

    // Create and start the streaming RPC
    let currentTxnVersion = config.starting_version || 0;
    const stream = client.getTransactions(request, metadata);

    const timer = new Timer();
    timer.start();

    stream.on("data", async function (response: TransactionsResponse) {
      stream.pause();
      const transactionsList = response.getTransactionsList();

      if (transactionsList == null) {
        return;
      }

      console.log({
        message: "Response received",
        starting_version: transactionsList[0].getVersion(),
      });

      // Validate response chain ID matches expected chain ID
      if (response.getChainId() != config.chain_id.toString()) {
        throw new Error(
          `Chain ID mismatch. Expected ${
            config.chain_id
          } but got ${response.getChainId()}`
        );
      }

      if (transactionsList == null) {
        return;
      }

      for (const transaction of transactionsList) {
        // Validate transaction version is correct
        if (transaction.getVersion() != currentTxnVersion.toString()) {
          throw new Error(
            `Transaction version mismatch. Expected ${currentTxnVersion} but got ${transaction.getVersion()}`
          );
        }

        const parsedObjs = parse(transaction);
        if (parsedObjs.length > 0) {
          await dataSource.transaction(async (txnManager) => {
            await txnManager.insert(Event, parsedObjs);
            const nextVersionToProcess = createNextVersionToProcess(
              currentTxnVersion + 1
            );
            await txnManager.upsert(
              NextVersionToProcess,
              nextVersionToProcess,
              ["indexerName"]
            );
          });
        } else if (currentTxnVersion % 1000 == 0) {
          // check point
          const nextVersionToProcess = createNextVersionToProcess(
            currentTxnVersion + 1
          );
          await dataSource
            .getRepository(NextVersionToProcess)
            .upsert(nextVersionToProcess, ["indexerName"]);

          console.log({
            message: "Successfully processed transaction",
            last_success_transaction_version: currentTxnVersion,
          });
        }

        const totalTransactions =
          currentTxnVersion - config.starting_version + 1;

        if (perf && totalTransactions >= Number(perf)) {
          timer.stop();
          console.log(
            `Takes ${timer.ms()} ms to receive ${totalTransactions} txns`
          );
          exit(0);
        }

        currentTxnVersion += 1;
      }
      stream.resume();
    });

    stream.on("error", function (e) {
      console.error(e);
      // An error has occurred and the stream has been closed.
    });
    stream.on("status", function (status) {
      console.log(status);
      // process status
    });
  });

function createNextVersionToProcess(version: number) {
  const nextVersionToProcess = new NextVersionToProcess();
  nextVersionToProcess.nextVersion = version.toString();
  nextVersionToProcess.indexerName = INDEXER_NAME;
  return nextVersionToProcess;
}

program.parse();
