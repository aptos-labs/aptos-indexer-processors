from config import Config
from create_table import LatestProcessedVersion
from event_parser import INDEXER_NAME, parse
from aptos.datastream.v1 import datastream_pb2_grpc

import grpc
from aptos.datastream.v1 import datastream_pb2
from aptos.transaction.testing1.v1 import transaction_pb2

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import argparse
import base64
import json


parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="Path to config file", required=True)
args = parser.parse_args()
config = Config.from_yaml_file(args.config)

metadata = (("x-aptos-data-authorization", config.indexer_api_key),)
options = [("grpc.max_receive_message_length", -1)]
engine = create_engine(config.db_connection_uri)

starting_version = 0
if config.starting_version != None:
    # Start from config's starting version if set
    starting_version = config.starting_version
else:
    # Start from latest processed version in db
    with Session(engine) as session, session.begin():
        latest_processed_version_from_db = session.get(
            LatestProcessedVersion, INDEXER_NAME
        )
        if latest_processed_version_from_db != None:
            starting_version = latest_processed_version_from_db.latest_processed_version

# Connect to grpc
with grpc.insecure_channel(config.indexer_endpoint, options=options) as channel:
    stub = datastream_pb2_grpc.IndexerStreamStub(channel)
    current_transaction_version = starting_version

    for response in stub.RawDatastream(
        datastream_pb2.RawDatastreamRequest(starting_version=starting_version),
        metadata=metadata,
    ):
        chain_id = response.chain_id

        if chain_id != config.chain_id:
            raise Exception(
                "Chain ID mismatch. Expected chain ID is: "
                + str(config.chain_id)
                + ", but received chain ID is: "
                + str(chain_id)
            )

        transactions_output = response.data
        for transaction_output in transactions_output.transactions:
            # Decode transaction data
            decoded_transaction = base64.b64decode(
                transaction_output.encoded_proto_data
            )
            transaction = transaction_pb2.Transaction()
            transaction.ParseFromString(decoded_transaction)

            transaction_version = transaction.version
            if transaction_version != current_transaction_version:
                raise Exception(
                    "Transaction version mismatch. Expected transaction version is: "
                    + str(current_transaction_version)
                    + ", but received transaction version is: "
                    + str(transaction_version)
                )

            parsed_objs = parse(transaction)

            with Session(engine) as session, session.begin():
                # Insert Events into database
                if parsed_objs is not None:
                    session.add_all(parsed_objs)

                # Update latest processed version
                session.merge(
                    LatestProcessedVersion(
                        indexer_name=INDEXER_NAME,
                        latest_processed_version=current_transaction_version,
                    )
                )

            if (current_transaction_version % 1000) == 0:
                print(
                    json.dumps(
                        {
                            "message": "Successfully processed transaction",
                            "last_success_transaction_version": current_transaction_version,
                        }
                    )
                )

            current_transaction_version += 1
