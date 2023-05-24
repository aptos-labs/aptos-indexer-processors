// package: aptos.indexer.v1
// file: aptos/indexer/v1/raw_data.proto

/* tslint:disable */
/* eslint-disable */

import * as grpc from "@grpc/grpc-js";
import * as aptos_indexer_v1_raw_data_pb from "../../../aptos/indexer/v1/raw_data_pb";
import * as aptos_transaction_v1_transaction_pb from "../../../aptos/transaction/v1/transaction_pb";

interface IRawDataService extends grpc.ServiceDefinition<grpc.UntypedServiceImplementation> {
    getTransactions: IRawDataService_IGetTransactions;
}

interface IRawDataService_IGetTransactions extends grpc.MethodDefinition<aptos_indexer_v1_raw_data_pb.GetTransactionsRequest, aptos_indexer_v1_raw_data_pb.TransactionsResponse> {
    path: "/aptos.indexer.v1.RawData/GetTransactions";
    requestStream: false;
    responseStream: true;
    requestSerialize: grpc.serialize<aptos_indexer_v1_raw_data_pb.GetTransactionsRequest>;
    requestDeserialize: grpc.deserialize<aptos_indexer_v1_raw_data_pb.GetTransactionsRequest>;
    responseSerialize: grpc.serialize<aptos_indexer_v1_raw_data_pb.TransactionsResponse>;
    responseDeserialize: grpc.deserialize<aptos_indexer_v1_raw_data_pb.TransactionsResponse>;
}

export const RawDataService: IRawDataService;

export interface IRawDataServer extends grpc.UntypedServiceImplementation {
    getTransactions: grpc.handleServerStreamingCall<aptos_indexer_v1_raw_data_pb.GetTransactionsRequest, aptos_indexer_v1_raw_data_pb.TransactionsResponse>;
}

export interface IRawDataClient {
    getTransactions(request: aptos_indexer_v1_raw_data_pb.GetTransactionsRequest, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<aptos_indexer_v1_raw_data_pb.TransactionsResponse>;
    getTransactions(request: aptos_indexer_v1_raw_data_pb.GetTransactionsRequest, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<aptos_indexer_v1_raw_data_pb.TransactionsResponse>;
}

export class RawDataClient extends grpc.Client implements IRawDataClient {
    constructor(address: string, credentials: grpc.ChannelCredentials, options?: Partial<grpc.ClientOptions>);
    public getTransactions(request: aptos_indexer_v1_raw_data_pb.GetTransactionsRequest, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<aptos_indexer_v1_raw_data_pb.TransactionsResponse>;
    public getTransactions(request: aptos_indexer_v1_raw_data_pb.GetTransactionsRequest, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<aptos_indexer_v1_raw_data_pb.TransactionsResponse>;
}
