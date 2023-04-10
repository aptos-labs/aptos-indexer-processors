from grpc_parser import parse
from aptos.datastream.v1 import datastream_pb2_grpc

import grpc
from aptos.datastream.v1 import datastream_pb2
from aptos.transaction.testing1.v1 import transaction_pb2

from google import auth as google_auth
from google.auth.transport import grpc as google_auth_transport_grpc
from google.auth.transport import requests as google_auth_transport_requests

metadata = (("x-aptos-data-authorization", "YOUR_TOKEN"),)
options = [('grpc.max_receive_message_length', -1)]
MAINNET_INDEXER_ENDPOINT = '34.70.26.67:50051'

with grpc.insecure_channel(MAINNET_INDEXER_ENDPOINT, options=options) as channel:
    stub = datastream_pb2_grpc.IndexerStreamStub(channel)
    for response in stub.RawDatastream(datastream_pb2.RawDatastreamRequest(starting_version=10000), metadata=metadata):
        transactions_output = response.data
        for transaction_output in transactions_output.transactions:
            # Decode transaction data
            transaction = transaction_pb2.Transaction()
            transaction.ParseFromString(transaction_output.encoded_proto_data)

            parsed_transaction = parse(transaction)
