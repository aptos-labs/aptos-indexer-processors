import { ArgumentParser } from 'argparse';
import { parse } from './grpc_parser';
import * as services from './aptos/indexer/v1/raw_data_grpc_pb';
import * as indexerRawDataMessages from './aptos/indexer/v1/raw_data_pb';
import { Config } from './config';
import * as grpc from '@grpc/grpc-js';
import { Timer } from 'timer-node';
import { exit } from 'process';
import { ChannelCredentials, CallCredentials } from '@grpc/grpc-js';

// A hack to override the http2 settings
class CustomChannelCred extends ChannelCredentials {
  constructor(callCredentials?: CallCredentials) {
    super();
  }

  compose(callCredentials: CallCredentials): never {
    throw new Error('Cannot compose insecure credentials');
  }

  _getConnectionOptions(): any {
    return {
      settings: {
        // This will increase the http2 frame size. Default is 16384, which is too small.
        maxFrameSize: 4194304,
        initialWindowSize: 4194304,
        maxHeaderListSize: 8192,
        enablePush: false,
        maxConcurrentStreams: 0,
        enableConnectProtocol: true,
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

// Parse the config file
const parser = new ArgumentParser({
  description: 'Indexer Stream Client arguments',
});
parser.add_argument('-c', '--config', {
  help: 'Path to config file',
  required: true,
});
const args = parser.parse_args();
const config = Config.from_yaml_file(args.config);

// Create client and request
const client = new services.RawDataClient(
  config.indexer_endpoint,
  new CustomChannelCred(),
  {
    'grpc.keepalive_time_ms': 1000,
    'grpc.default_compression_algorithm': 2,
    'grpc.default_compression_level': 3,
    'grpc.max_receive_message_length': -1,
    'grpc.max_send_message_length': -1,
  }
);

const request = new indexerRawDataMessages.GetTransactionsRequest();
request.setStartingVersion(config.starting_version);
const metadata = new grpc.Metadata();
metadata.set('x-aptos-data-authorization', config.indexer_api_key);

// Create and start the streaming RPC
let currentTransactionVersion = config.starting_version;
const stream = client.getTransactions(request, metadata);

const timer = new Timer();
timer.start();

stream.on(
  'data',
  function (response: indexerRawDataMessages.TransactionsResponse) {
    const transactionsList = response.getTransactionsList();

    if (transactionsList == null) {
      return;
    }

    // Validate response chain ID matches expected chain ID
    if (response.getChainId() != config.chain_id) {
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
      if (transaction.getVersion() != currentTransactionVersion) {
        throw new Error(
          `Transaction version mismatch. Expected ${currentTransactionVersion} but got ${transaction.getVersion()}`
        );
      }

      parse(transaction);

      if (currentTransactionVersion - config.starting_version + 1 >= 25000) {
        console.log({
          message: 'Successfully processed transaction',
          last_success_transaction_version: currentTransactionVersion,
        });
        // TODO: remove this
        timer.stop();
        console.log(
          `Time elapsed: ${timer.ms()} ms for ${
            currentTransactionVersion - config.starting_version + 1
          } txns`
        );
        exit(0);
      }

      currentTransactionVersion += 1;
    }
  }
);

stream.on('error', function (e) {
  console.log(e);
  // An error has occurred and the stream has been closed.
});
stream.on('status', function (status) {
  console.log(status);
  // process status
});
