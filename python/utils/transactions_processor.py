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
import traceback
from abc import ABC, abstractmethod


class TransactionsProcessor(ABC):
    parser_function: Callable[[transaction_pb2.Transaction], list[Any]]
    config: Config
    engine: Engine | None

    def __init__(
        self,
    ):
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", help="Path to config file", required=True)
        args = parser.parse_args()
        self.config = Config.from_yaml_file(args.config)
        # self.parser_function = parser_function
        # self.processor_name = processor_name
        # self.schema_name = schema_name

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

        self.init_db_tables()

    # Name of the processor for status logging
    # This will get stored in the database for each (`TransactionProcessor`, transaction_version) pair
    @abstractmethod
    def name(self) -> str:
        pass

    # Name of the DB schema this processor writes to
    @abstractmethod
    def schema(self) -> str:
        pass

    # Process all transactions within a block and processes it.
    # This method will be called from `process_transaction_with_status`
    # In case a transaction cannot be processed, we will fail the entire block.
    @abstractmethod
    def process_transactions(
        self,
        transactions: list[transaction_pb2.Transaction],
        start_version: int,
        end_version: int,
    ) -> None:
        # TODO: Make this return processing result
        pass

    # TODO: Move this to indexer worker
    def init_db_tables(self) -> None:
        engine = create_engine(self.config.db_connection_uri)
        engine = engine.execution_options(
            schema_translate_map={"per_schema": self.schema()}
        )
        Session.configure(bind=engine)
        Base.metadata.create_all(engine, checkfirst=True)

    # TODO: Move this to indexer worker
    def run(self) -> None:
        # Setup the GetTransactionsRequest

        processor_name = self.name()
        starting_version = self.config.get_starting_version(processor_name)
        ending_version = self.config.ending_version

        request = raw_data_pb2.GetTransactionsRequest(starting_version=starting_version)

        # Setup GRPC settings
        metadata = (
            ("x-aptos-data-authorization", self.config.grpc_data_stream_api_key),
            ("x-aptos-request-name", processor_name),
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
        # TODO: Move this to indexer worker
        with grpc.insecure_channel(
            self.config.grpc_data_stream_endpoint, options=options
        ) as channel:
            stub = raw_data_pb2_grpc.RawDataStub(channel)

            last_processed_version = None

            try:
                for response in stub.GetTransactions(
                    request,
                    metadata=metadata,
                ):
                    self.validate_grpc_chain_id(response)

                    batch_start_version = response.transactions[0].version
                    batch_end_version = response.transactions[-1].version

                    print(
                        json.dumps(
                            {
                                "message": "[Parser] Response received",
                                "starting_version": response.transactions[0].version,
                            }
                        )
                    )

                    # TODO: Transaction version validation

                    # If ending version is in the current batch, truncate the transactions in this batcch
                    if ending_version != None and batch_end_version >= ending_version:
                        batch_end_version = ending_version

                    self.process_transactions(
                        response.transactions[
                            : batch_end_version - batch_start_version + 1
                        ],
                        batch_start_version,
                        batch_end_version,
                    )

                    # Stop processing if reached ending version
                    # TODO: Maybe move this to indexer worker
                    if ending_version == batch_end_version:
                        print(
                            json.dumps(
                                {
                                    "message": "Reached ending version",
                                    "ending_version": ending_version,
                                }
                            )
                        )
                        return

                    if batch_end_version % 1000 == 0:
                        print(
                            json.dumps(
                                {
                                    "message": "Successfully processed transaction",
                                    "last_processed_version": batch_end_version,
                                }
                            )
                        )

                    # Update last_processed_version for error logging
                    last_processed_version = batch_end_version
            except grpc.RpcError as e:
                print(
                    json.dumps(
                        {
                            "message": "[Parser] Error processing transaction",
                            "last_success_transaction_version": last_processed_version,
                            "error": str(e),
                        }
                    )
                )
                # TODO: Add retry logic.
                sys.exit(1)

    def update_last_processed_version(self, last_processed_version) -> None:
        with Session() as session, session.begin():
            session.merge(
                NextVersionToProcess(
                    indexer_name=self.name(),
                    next_version=last_processed_version + 1,
                )
            )

    # TODO: Move this to worker and validate chain id same way as Rust
    def validate_grpc_chain_id(self, response: raw_data_pb2.TransactionsResponse):
        chain_id = response.chain_id

        if chain_id != self.config.chain_id:
            raise Exception(
                "Chain ID mismatch. Expected chain ID is: "
                + str(self.config.chain_id)
                + ", but received chain ID is: "
                + str(chain_id)
            )


# TODO: Move DB schema initialization to the worker
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
