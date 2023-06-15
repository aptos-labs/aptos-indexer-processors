import argparse
import grpc
import json

from collections import defaultdict
from utils.models.general_models import NextVersionToProcess
from aptos.indexer.v1 import raw_data_pb2, raw_data_pb2_grpc
from aptos.transaction.v1 import transaction_pb2
from utils.config import Config
from utils.models.general_models import Base
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session
from typing import Any, Callable


class TransactionsProcessor:
    def __init__(self, parser_function: Callable[[transaction_pb2.Transaction], Any]):
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", help="Path to config file", required=True)
        args = parser.parse_args()
        self.config = Config.from_yaml_file(args.config)
        self.parser_function = parser_function
        self.engine = None

        self.init_db_tables()

    def init_db_tables(self) -> None:
        self.engine = create_engine(self.config.db_connection_uri)
        Base.metadata.create_all(self.engine, checkfirst=True)

        # # Check if the tables are created.
        # inspector = inspect(self.engine)
        # table_name = NextVersionToProcess.__tablename__
        # if inspector.has_table(table_name):
        #     print("Table {} exists.".format(table_name))
        # else:
        #     print("Table {} does not exist. Creating it now.".format(table_name))
        #     Base.metadata.create_all(self.engine)

    def process(self) -> None:
        # Setup the GetTransactionsRequest
        starting_version = self.config.get_starting_version()
        request = raw_data_pb2.GetTransactionsRequest(starting_version=starting_version)

        # Setup GRPC settings
        metadata = (
            ("x-aptos-data-authorization", self.config.indexer_api_key),
            ("x-aptos-request-name", self.config.indexer_name),
        )
        options = [("grpc.max_receive_message_length", -1)]

        print(
            json.dumps(
                {
                    "message": "Connected to the indexer grpc",
                    "starting_version": starting_version,
                }
            )
        )

        # Connect to indexer grpc endpoint
        with grpc.insecure_channel(
            self.config.indexer_endpoint, options=options
        ) as channel:
            stub = raw_data_pb2_grpc.RawDataStub(channel)
            current_transaction_version = starting_version

            for response in stub.GetTransactions(
                request,
                metadata=metadata,
            ):
                chain_id = response.chain_id

                if chain_id != self.config.chain_id:
                    raise Exception(
                        "Chain ID mismatch. Expected chain ID is: "
                        + str(self.config.chain_id)
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

                    if self.config.ending_version != None:
                        if transaction_version > self.config.ending_version:
                            print(
                                json.dumps(
                                    {
                                        "message": "Reached ending version",
                                        "ending_version": self.config.ending_version,
                                    }
                                )
                            )
                            return

                    parsed_objs = self.parser_function(transaction)
                    self.insert_to_db(parsed_objs, current_transaction_version)

                    if current_transaction_version % 1000 == 0:
                        print(
                            json.dumps(
                                {
                                    "message": "Successfully processed transaction",
                                    "last_success_transaction_version": current_transaction_version,
                                }
                            )
                        )

                    current_transaction_version += 1

    def insert_to_db(self, parsed_objs, txn_version) -> None:
        indexer_name = self.config.indexer_name

        # If we find relevant transactions add them and update latest processed version
        if parsed_objs is not None:
            with Session(self.engine) as session, session.begin():
                for obj in parsed_objs:
                    session.merge(obj)

                # Update latest processed version
                session.merge(
                    NextVersionToProcess(
                        indexer_name=indexer_name,
                        next_version=txn_version + 1,
                    )
                )
        # If we don't find any relevant transactions, at least update latest processed version every 1000
        elif (txn_version % 1000) == 0:
            with Session(self.engine) as session, session.begin():
                # Update latest processed version
                session.merge(
                    NextVersionToProcess(
                        indexer_name=indexer_name,
                        next_version=txn_version + 1,
                    )
                )
