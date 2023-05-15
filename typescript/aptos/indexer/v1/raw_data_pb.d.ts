// package: aptos.indexer.v1
// file: aptos/indexer/v1/raw_data.proto

/* tslint:disable */
/* eslint-disable */

import * as jspb from "google-protobuf";
import * as aptos_transaction_testing1_v1_transaction_pb from "../../../aptos/transaction/testing1/v1/transaction_pb";

export class GetTransactionsRequest extends jspb.Message { 

    hasStartingVersion(): boolean;
    clearStartingVersion(): void;
    getStartingVersion(): number | undefined;
    setStartingVersion(value: number): GetTransactionsRequest;

    hasTransactionsCount(): boolean;
    clearTransactionsCount(): void;
    getTransactionsCount(): number | undefined;
    setTransactionsCount(value: number): GetTransactionsRequest;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): GetTransactionsRequest.AsObject;
    static toObject(includeInstance: boolean, msg: GetTransactionsRequest): GetTransactionsRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: GetTransactionsRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): GetTransactionsRequest;
    static deserializeBinaryFromReader(message: GetTransactionsRequest, reader: jspb.BinaryReader): GetTransactionsRequest;
}

export namespace GetTransactionsRequest {
    export type AsObject = {
        startingVersion?: number,
        transactionsCount?: number,
    }
}

export class TransactionsResponse extends jspb.Message { 
    clearTransactionsList(): void;
    getTransactionsList(): Array<aptos_transaction_testing1_v1_transaction_pb.Transaction>;
    setTransactionsList(value: Array<aptos_transaction_testing1_v1_transaction_pb.Transaction>): TransactionsResponse;
    addTransactions(value?: aptos_transaction_testing1_v1_transaction_pb.Transaction, index?: number): aptos_transaction_testing1_v1_transaction_pb.Transaction;

    hasChainId(): boolean;
    clearChainId(): void;
    getChainId(): number | undefined;
    setChainId(value: number): TransactionsResponse;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): TransactionsResponse.AsObject;
    static toObject(includeInstance: boolean, msg: TransactionsResponse): TransactionsResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: TransactionsResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): TransactionsResponse;
    static deserializeBinaryFromReader(message: TransactionsResponse, reader: jspb.BinaryReader): TransactionsResponse;
}

export namespace TransactionsResponse {
    export type AsObject = {
        transactionsList: Array<aptos_transaction_testing1_v1_transaction_pb.Transaction.AsObject>,
        chainId?: number,
    }
}
