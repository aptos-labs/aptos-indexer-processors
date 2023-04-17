// package: aptos.datastream.v1
// file: aptos/datastream/v1/datastream.proto

var aptos_datastream_v1_datastream_pb = require("../../../aptos/datastream/v1/datastream_pb");
var grpc = require("@improbable-eng/grpc-web").grpc;

var IndexerStream = (function () {
  function IndexerStream() {}
  IndexerStream.serviceName = "aptos.datastream.v1.IndexerStream";
  return IndexerStream;
}());

IndexerStream.RawDatastream = {
  methodName: "RawDatastream",
  service: IndexerStream,
  requestStream: false,
  responseStream: true,
  requestType: aptos_datastream_v1_datastream_pb.RawDatastreamRequest,
  responseType: aptos_datastream_v1_datastream_pb.RawDatastreamResponse
};

exports.IndexerStream = IndexerStream;

function IndexerStreamClient(serviceHost, options) {
  this.serviceHost = serviceHost;
  this.options = options || {};
}

IndexerStreamClient.prototype.rawDatastream = function rawDatastream(requestMessage, metadata) {
  var listeners = {
    data: [],
    end: [],
    status: []
  };
  var client = grpc.invoke(IndexerStream.RawDatastream, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onMessage: function (responseMessage) {
      listeners.data.forEach(function (handler) {
        handler(responseMessage);
      });
    },
    onEnd: function (status, statusMessage, trailers) {
      listeners.status.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners.end.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners = null;
    }
  });
  return {
    on: function (type, handler) {
      listeners[type].push(handler);
      return this;
    },
    cancel: function () {
      listeners = null;
      client.close();
    }
  };
};

exports.IndexerStreamClient = IndexerStreamClient;

