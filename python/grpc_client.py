from grpc_parser import parse
from aptos.datastream.v1 import datastream_pb2_grpc

import grpc
from aptos.datastream.v1 import datastream_pb2
from aptos.transaction.testing1.v1 import transaction_pb2

from google import auth as google_auth
from google.auth.transport import grpc as google_auth_transport_grpc
from google.auth.transport import requests as google_auth_transport_requests

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import base64
import datetime
import yaml

with open('../config.yaml', 'r') as file:
    config = yaml.safe_load(file)

metadata = (("x-aptos-data-authorization", config["x-aptos-data-authorization"]),)
options = [('grpc.max_receive_message_length', -1)]
engine = create_engine(config['tablename'])

with grpc.insecure_channel(config["indexer-endpoint"], options=options) as channel:
    stub = datastream_pb2_grpc.IndexerStreamStub(channel)
    current_transaction_version = config["starting-version"]

    for response in stub.RawDatastream(datastream_pb2.RawDatastreamRequest(starting_version=config["starting-version"]), metadata=metadata):
        chain_id = response.chain_id

        if chain_id != config["chain-id"]:
            raise Exception("Chain ID mismatch. Expected chain ID is: " + str(config["chain-id"]) + ", but received chain ID is: " + str(chain_id))
        
        transactions_output = response.data
        for transaction_output in transactions_output.transactions:
            # Decode transaction data
            decoded_transaction = base64.b64decode(transaction_output.encoded_proto_data)
            transaction = transaction_pb2.Transaction()
            transaction.ParseFromString(decoded_transaction)

            transaction_version = transaction.version
            if transaction_version != current_transaction_version:
                raise Exception("Transaction version mismatch. Expected transaction version is: " + str(current_transaction_version) + ", but received transaction version is: " + str(transaction_version))

            current_transaction_version += 1
            parsed_objs = parse(transaction)

            if parsed_objs is not None:
                # Insert objects into database
                with Session(engine) as session, session.begin():
                    session.add_all(parsed_objs)

            # Keep track of last successfully processed transaction version
            cursor_file = open(config["cursor-filename"], 'w+')
            cursor_file.write("last_success_transaction_version=" + str(current_transaction_version) + "\n")
            cursor_file.write("last_updated=" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
            cursor_file.close()                
