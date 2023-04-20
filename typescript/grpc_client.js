"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var argparse_1 = require("argparse");
var config_1 = require("./config");
var messages = require("./aptos/datastream/v1/datastream_pb");
var grpc = require("@grpc/grpc-js");
var services = require("./aptos/datastream/v1/datastream_grpc_pb");
// Parse the config file
var parser = new argparse_1.ArgumentParser({
    description: "Indexer Stream Client arguments",
});
parser.add_argument("-c", "--config", {
    help: "Path to config file",
    required: true,
});
var args = parser.parse_args();
var config = config_1.Config.from_yaml_file(args.config);
// Create client and request
var client = new services.IndexerStreamClient(config.indexer_endpoint, grpc.credentials.createInsecure());
var request = new messages.RawDatastreamRequest();
request.setStartingVersion(config.starting_version);
var metadata = new grpc.Metadata();
metadata.set("x-aptos-data-authorization", config.indexer_api_key);
// Create and start the streaming RPC
var stream = client.rawDatastream(request, metadata);
stream.on("data", function (response) {
    console.log(response.toObject());
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
