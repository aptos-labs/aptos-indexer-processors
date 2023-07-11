import argparse
import grpc
import json

from utils.models.general_models import NextVersionToProcess
from aptos.indexer.v1 import raw_data_pb2, raw_data_pb2_grpc
from aptos.transaction.v1 import transaction_pb2
from utils.config import Config
from utils.models.general_models import Base
from utils.session import Session
from utils.metrics import PROCESSED_TRANSACTIONS_COUNTER
from sqlalchemy import DDL, Engine, create_engine
from sqlalchemy import event
from typing import Any, Callable
from prometheus_client.twisted import MetricsResource
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet import reactor
import http.server
import socketserver
import threading
import sys


class TransactionsProcessor:
    parser_function: Callable[[transaction_pb2.Transaction], list[Any]]
    config: Config
    processor_name: str
    engine: Engine | None

    def __init__(
        self,
        parser_function: Callable[[transaction_pb2.Transaction], list[Any]],
        processor_name: str,
    ):
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", help="Path to config file", required=True)
        args = parser.parse_args()
        self.config = Config.from_yaml_file(args.config)
        self.parser_function = parser_function
        self.processor_name = processor_name

        self.init_db_tables()

        # Start the health + metrics server.
        def start_health_server() -> None:
            # The kubelet uses liveness probes to know when to restart a container. In cases where the
            # container is crashing or unresponsive, the kubelet receives timeout or error responses, and then
            # restarts the container. It polls every 10 seconds by default.
            root = Resource()
            root.putChild(b"metrics", MetricsResource())  # type: ignore

            class ServerOk(Resource):
                isLeaf = True

                def render_GET(self, request):
                    return b"ok"

            root.putChild(b"", ServerOk())  # type: ignore
            factory = Site(root)
            reactor.listenTCP(self.config.health_port, factory)  # type: ignore
            reactor.run(installSignalHandlers=False)  # type: ignore

        t = threading.Thread(target=start_health_server, daemon=True)
        # TODO: Handles the exit signal and gracefully shutdown the server.
        t.start()

    def init_db_tables(self) -> None:
        engine = create_engine(self.config.db_connection_uri)
        Session.configure(bind=engine)
        Base.metadata.create_all(engine, checkfirst=True)

    def process(self) -> None:
        # Setup the GetTransactionsRequest

        starting_version = self.config.get_starting_version(self.processor_name)

        request = raw_data_pb2.GetTransactionsRequest(starting_version=starting_version)

        # Setup GRPC settings
        metadata = (
            ("x-aptos-data-authorization", self.config.grpc_data_stream_api_key),
            ("x-aptos-request-name", self.processor_name),
        )
        options = [
            ("grpc.max_receive_message_length", -1),
            (
                "grpc.keepalive_time_ms",
                self.config.indexer_grpc_http2_ping_interval_in_secs * 1000,
            ),
            (
                "grpc.keepalive_timeout_ms",
                self.config.indexer_grpc_http2_ping_timeout_in_secs * 1000,
            ),
        ]

        print(
            json.dumps(
                {
                    "message": f"Connected to grpc data stream endpoint: {self.config.grpc_data_stream_endpoint}",
                    "starting_version": starting_version,
                }
            ),
            flush=True,
        )

        # Connect to indexer grpc endpoint
        with grpc.insecure_channel(
            self.config.grpc_data_stream_endpoint, options=options
        ) as channel:
            stub = raw_data_pb2_grpc.RawDataStub(channel)
            current_transaction_version = starting_version
            try:
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
                        PROCESSED_TRANSACTIONS_COUNTER.inc()
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
            except grpc.RpcError as e:
                print(
                    json.dumps(
                        {
                            "message": "Error processing transaction",
                            "last_success_transaction_version": current_transaction_version,
                            "error": str(e),
                        }
                    )
                )
                # TODO: Add retry logic.
                sys.exit(1)

    def insert_to_db(self, parsed_objs, txn_version) -> None:
        # If we find relevant transactions add them and update latest processed version
        if parsed_objs is not None:
            with Session() as session, session.begin():
                for obj in parsed_objs:
                    session.merge(obj)

                # Update latest processed version
                session.merge(
                    NextVersionToProcess(
                        indexer_name=self.processor_name,
                        next_version=txn_version + 1,
                    )
                )
        # If we don't find any relevant transactions, at least update latest processed version every 1000
        elif (txn_version % 1000) == 0:
            with Session() as session, session.begin():
                # Update latest processed version
                session.merge(
                    NextVersionToProcess(
                        indexer_name=self.processor_name,
                        next_version=txn_version + 1,
                    )
                )


@event.listens_for(Base.metadata, "before_create")
def create_schemas(target, connection, **kw):
    schemas = set()
    for table in target.tables.values():
        if table.schema is not None:
            schemas.add(table.schema)
    for schema in schemas:
        connection.execute(DDL("CREATE SCHEMA IF NOT EXISTS %s" % schema))


@event.listens_for(Base.metadata, "after_drop")
def drop_schemas(target, connection, **kw):
    schemas = set()
    for table in target.tables.values():
        if table.schema is not None:
            schemas.add(table.schema)
    for schema in schemas:
        connection.execute(DDL("DROP SCHEMA IF EXISTS %s" % schema))
