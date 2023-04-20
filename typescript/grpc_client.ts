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
  grpc.credentials.createInsecure()
);

const request = new messages.RawDatastreamRequest();
request.setStartingVersion(config.starting_version);
const metadata = new grpc.Metadata();
metadata.set("x-aptos-data-authorization", config.indexer_api_key);

// Create and start the streaming RPC
const stream = client.rawDatastream(request, metadata);
stream.on("data", function (response) {
  console.log("got response");
});
stream.on("end", function () {
  // The server has finished sending
});
stream.on("error", function (e) {
  // An error has occurred and the stream has been closed.
});
stream.on("status", function (status) {
  // process status
});
