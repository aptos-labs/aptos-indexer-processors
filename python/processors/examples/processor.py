from config import Config
from create_table import NextVersionToProcess
from event_parser import INDEXER_NAME, parse
from aptos.indexer.v1 import raw_data_pb2_grpc

import grpc
from aptos.indexer.v1 import raw_data_pb2

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import argparse
import json


parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="Path to config file", required=True)
args = parser.parse_args()
config = Config.from_yaml_file(args.config)

metadata = (
    ("x-aptos-data-authorization", config.indexer_api_key),
    ("x-aptos-request-name", INDEXER_NAME),
)
options = [("grpc.max_receive_message_length", -1)]
engine = create_engine(config.db_connection_uri)

# By default, if nothing is set, start from 0
starting_version = 0
if config.starting_version_override != None:
    # Start from config's starting_version_override if set
    starting_version = config.starting_version_override
else:
    with Session(engine) as session, session.begin():
        next_version_to_process_from_db = session.get(
            NextVersionToProcess, INDEXER_NAME
        )
        if next_version_to_process_from_db != None:
            # Start from next version to process in db
            starting_version = next_version_to_process_from_db.next_version
        elif config.starting_version_default != None:
            # Start from config's starting_version_default if set
            starting_version = config.starting_version_default

print(
    json.dumps(
        {
            "message": "Connected to the indexer grpc",
            "starting_version": starting_version,
        }
    )
)
# Connect to grpc
with grpc.insecure_channel(config.indexer_endpoint, options=options) as channel:
    stub = raw_data_pb2_grpc.RawDataStub(channel)
    current_transaction_version = starting_version

    for response in stub.GetTransactions(
        raw_data_pb2.GetTransactionsRequest(starting_version=starting_version),
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
        print(
            json.dumps(
                {
                    "message": "Response received",
                    "starting_version": response.transactions[0].version,
                }
            )
        )
        transactions_output = response
        for transaction in transactions_output.transactions:
            transaction_version = transaction.version
            if transaction_version != current_transaction_version:
                raise Exception(
                    "Transaction version mismatch. Expected transaction version is: "
                    + str(current_transaction_version)
                    + ", but received transaction version is: "
                    + str(transaction_version)
                )

            parsed_objs = parse(transaction)

            # If we find relevant transactions add them and update latest processed version
            if parsed_objs is not None:
                with Session(engine) as session, session.begin():
                    # Insert Events into database
                    session.add_all(parsed_objs)

                    # Update latest processed version
                    session.merge(
                        NextVersionToProcess(
                            indexer_name=INDEXER_NAME,
                            next_version=current_transaction_version + 1,
                        )
                    )
            # If we don't find any relevant transactions, at least update latest processed version every 1000
            elif (current_transaction_version % 1000) == 0:
                with Session(engine) as session, session.begin():
                    # Update latest processed version
                    session.merge(
                        NextVersionToProcess(
                            indexer_name=INDEXER_NAME,
                            next_version=current_transaction_version + 1,
                        )
                    )
                print(
                    json.dumps(
                        {
                            "message": "Successfully processed transaction",
                            "last_success_transaction_version": current_transaction_version,
                        }
                    )
                )

            current_transaction_version += 1
