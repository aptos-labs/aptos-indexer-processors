// package: aptos.datastream.v1
// file: aptos/datastream/v1/datastream.proto

import * as aptos_datastream_v1_datastream_pb from "../../../aptos/datastream/v1/datastream_pb";
import {grpc} from "@improbable-eng/grpc-web";

type IndexerStreamRawDatastream = {
  readonly methodName: string;
  readonly service: typeof IndexerStream;
  readonly requestStream: false;
  readonly responseStream: true;
  readonly requestType: typeof aptos_datastream_v1_datastream_pb.RawDatastreamRequest;
  readonly responseType: typeof aptos_datastream_v1_datastream_pb.RawDatastreamResponse;
};

export class IndexerStream {
  static readonly serviceName: string;
  static readonly RawDatastream: IndexerStreamRawDatastream;
}

export type ServiceError = { message: string, code: number; metadata: grpc.Metadata }
export type Status = { details: string, code: number; metadata: grpc.Metadata }

interface UnaryResponse {
  cancel(): void;
}
interface ResponseStream<T> {
  cancel(): void;
  on(type: 'data', handler: (message: T) => void): ResponseStream<T>;
  on(type: 'end', handler: (status?: Status) => void): ResponseStream<T>;
  on(type: 'status', handler: (status: Status) => void): ResponseStream<T>;
}
interface RequestStream<T> {
  write(message: T): RequestStream<T>;
  end(): void;
  cancel(): void;
  on(type: 'end', handler: (status?: Status) => void): RequestStream<T>;
  on(type: 'status', handler: (status: Status) => void): RequestStream<T>;
}
interface BidirectionalStream<ReqT, ResT> {
  write(message: ReqT): BidirectionalStream<ReqT, ResT>;
  end(): void;
  cancel(): void;
  on(type: 'data', handler: (message: ResT) => void): BidirectionalStream<ReqT, ResT>;
  on(type: 'end', handler: (status?: Status) => void): BidirectionalStream<ReqT, ResT>;
  on(type: 'status', handler: (status: Status) => void): BidirectionalStream<ReqT, ResT>;
}

export class IndexerStreamClient {
  readonly serviceHost: string;

  constructor(serviceHost: string, options?: grpc.RpcOptions);
  rawDatastream(requestMessage: aptos_datastream_v1_datastream_pb.RawDatastreamRequest, metadata?: grpc.Metadata): ResponseStream<aptos_datastream_v1_datastream_pb.RawDatastreamResponse>;
}

