// GENERATED CODE -- DO NOT EDIT!

// Original file comments:
// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
//
'use strict';
var grpc = require('@grpc/grpc-js');
var aptos_indexer_v1_raw_data_pb = require('../../../aptos/indexer/v1/raw_data_pb.js');
var aptos_transaction_testing1_v1_transaction_pb = require('../../../aptos/transaction/testing1/v1/transaction_pb.js');

function serialize_aptos_indexer_v1_GetTransactionsRequest(arg) {
  if (!(arg instanceof aptos_indexer_v1_raw_data_pb.GetTransactionsRequest)) {
    throw new Error('Expected argument of type aptos.indexer.v1.GetTransactionsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_aptos_indexer_v1_GetTransactionsRequest(buffer_arg) {
  return aptos_indexer_v1_raw_data_pb.GetTransactionsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_aptos_indexer_v1_TransactionsResponse(arg) {
  if (!(arg instanceof aptos_indexer_v1_raw_data_pb.TransactionsResponse)) {
    throw new Error('Expected argument of type aptos.indexer.v1.TransactionsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_aptos_indexer_v1_TransactionsResponse(buffer_arg) {
  return aptos_indexer_v1_raw_data_pb.TransactionsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


var RawDataService = exports.RawDataService = {
  // Get transactions batch without any filtering from starting version and end if transaction count is present.
getTransactions: {
    path: '/aptos.indexer.v1.RawData/GetTransactions',
    requestStream: false,
    responseStream: true,
    requestType: aptos_indexer_v1_raw_data_pb.GetTransactionsRequest,
    responseType: aptos_indexer_v1_raw_data_pb.TransactionsResponse,
    requestSerialize: serialize_aptos_indexer_v1_GetTransactionsRequest,
    requestDeserialize: deserialize_aptos_indexer_v1_GetTransactionsRequest,
    responseSerialize: serialize_aptos_indexer_v1_TransactionsResponse,
    responseDeserialize: deserialize_aptos_indexer_v1_TransactionsResponse,
  },
};

exports.RawDataClient = grpc.makeGenericClientConstructor(RawDataService);
