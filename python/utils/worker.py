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
from sqlalchemy import DDL, create_engine
from sqlalchemy import event
from typing import List
from prometheus_client.twisted import MetricsResource
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet import reactor
import threading
import sys
from utils.transactions_processor import TransactionsProcessor, ProcessingResult
from time import perf_counter
import traceback
from utils.processor_name import ProcessorName
from processors.example_event_processor.processor import ExampleEventProcessor
from processors.nft_orderbooks.nft_marketplace_processor import NFTMarketplaceProcesser
from processors.nft_marketplace_v2.processor import NFTMarketplaceV2Processor

INDEXER_GRPC_BLOB_STORAGE_SIZE = 1000


class IndexerProcessorServer:
    config: Config
    num_concurrent_processing_tasks: int

    def __init__(self, config: Config):
        print("[Parser] Kicking off")

        self.config = config

        # Instantiate the correct processor based on config
        match self.config.processor_name:
            case ProcessorName.EXAMPLE_EVENT_PROCESSOR.value:
                self.processor = ExampleEventProcessor()
            case ProcessorName.NFT_MARKETPLACE_V1_PROCESSOR.value:
                self.processor = NFTMarketplaceProcesser()
            case ProcessorName.NFT_MARKETPLACE_V2_PROCESSOR.value:
                self.processor = NFTMarketplaceV2Processor()
            case _:
                raise Exception(
                    "Invalid processor name"
                    "\n[ERROR]: The specified processor name was invalid or not found.\n"
                    "         - If you are using a custom processor, make sure to add it to the ProcessorName enum in utils/processor_name.py.\n"
                    "         - Ensure the IndexerProcessorServer constructor in utils/worker.py uses the new enum value.\n"
                )

        self.processor.config = self.config

        # TODO: Move this to a config
        self.num_concurrent_processing_tasks = 10

    class WorkerThread(
        threading.Thread,
    ):
        processing_result: ProcessingResult
        exception: Exception | None

        def __init__(
            self,
            processor: TransactionsProcessor,
            transactions: List[transaction_pb2.Transaction],
        ):
            threading.Thread.__init__(self)
            self.processor = processor
            self.transactions = transactions
            self.processing_result = ProcessingResult(
                transactions[0].version, transactions[-1].version
            )
            self.exception = None

        def run(self):
            start_version = self.transactions[0].version
            end_version = self.transactions[-1].version

            try:
                self.processing_result = self.processor.process_transactions(
                    self.transactions, start_version, end_version
                )

            except Exception as e:
                self.exception = e

    def run(self):
        # Run DB migrations
        print("[Parser] Initializing DB tables")
        self.init_db_tables(self.processor.schema())
        print("[Parser] DB tables initialized")

        self.start_health_and_monitoring_ports()

        # Get starting version from DB
        starting_version = self.config.get_starting_version(self.processor.name())
        ending_version = self.config.ending_version

        # Setup GRPC settings
        metadata = (
            ("x-aptos-data-authorization", self.config.grpc_data_stream_api_key),
            ("x-aptos-request-name", self.processor.name()),
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

        # Connect to indexer grpc endpoint
        with grpc.insecure_channel(
            self.config.grpc_data_stream_endpoint, options=options
        ) as channel:
            print(
                json.dumps(
                    {
                        "message": f"Connected to grpc data stream endpoint: {self.config.grpc_data_stream_endpoint}",
                        "starting_version": starting_version,
                    }
                ),
                flush=True,
            )

            batch_start_version = starting_version
            batch_end_version = None
            prev_processed_start_version = None
            prev_processed_end_version = None

            # Create GPRC request and get the responses
            stub = raw_data_pb2_grpc.RawDataStub(channel)
            request = raw_data_pb2.GetTransactionsRequest(
                starting_version=batch_start_version
            )
            responses = stub.GetTransactions(request, metadata=metadata)
            responses = iter(responses)

            while True:
                perf_start_time = perf_counter()
                transaction_batches: List[List[transaction_pb2.Transaction]] = []

                # Get batches of transactions for processing
                for _ in range(self.num_concurrent_processing_tasks):
                    response = next(responses)
                    transactions = response.transactions

                    current_batch_size = len(transactions)
                    if current_batch_size == 0:
                        print("[Parser] Received empty batch from GRPC stream")
                        sys.exit(1)

                    batch_start_version = transactions[0].version
                    batch_end_version = transactions[-1].version

                    # If ending version is in the current batch, truncate the transactions in this batcch
                    if ending_version != None and batch_end_version >= ending_version:
                        batch_end_version = ending_version
                        transactions = transactions[
                            : batch_end_version - batch_start_version + 1
                        ]
                        current_batch_size = len(transactions)

                    transaction_batches.append(transactions)

                    # If it is a partial batch, then skip polling and head to process it first.
                    if current_batch_size < INDEXER_GRPC_BLOB_STORAGE_SIZE:
                        break

                # Process transactions in batches
                threads: List[IndexerProcessorServer.WorkerThread] = []
                for transactions in transaction_batches:
                    thread = IndexerProcessorServer.WorkerThread(
                        self.processor, transactions
                    )
                    threads.append(thread)
                    thread.start()

                # Wait for processor threads to finish
                for thread in threads:
                    thread.join()

                # Update state depending on the results of the batch processing
                processed_versions: List[ProcessingResult] = []
                for thread in threads:
                    processing_result = thread.processing_result
                    exception = thread.exception

                    # TODO: Log errors metric
                    if thread.exception:
                        print(
                            json.dumps(
                                {
                                    "message": f"[Parser] Error processing transactions {processing_result.start_version} to {processing_result.end_version}",
                                    "error": str(exception),
                                    "error_stacktrace": traceback.format_exception(
                                        exception
                                    ),
                                }
                            )
                        )
                        sys.exit(1)
                    elif processing_result:
                        processed_versions.append(processing_result)

                # Make sure there are no gaps and advance states
                processed_versions.sort(key=lambda x: x.start_version)

                for version in processed_versions:
                    if prev_processed_start_version == None:
                        if version.start_version != starting_version:
                            print(
                                json.dumps(
                                    {
                                        "message": "[Parser] Detected gap in processed transactions",
                                        "error": f"Gap between transactions {starting_version} and {version.start_version}",
                                    }
                                )
                            )
                        prev_processed_start_version = version.start_version
                        prev_processed_end_version = version.end_version
                    else:
                        assert prev_processed_end_version
                        if prev_processed_end_version + 1 != version.start_version:
                            print(
                                json.dumps(
                                    {
                                        "message": "[Parser] Detected gap in processed transactions",
                                        "error": f"Gap between transactions {prev_processed_end_version} and {version.start_version}",
                                    }
                                )
                            )
                            sys.exit(1)
                        else:
                            prev_processed_start_version = version.start_version
                            prev_processed_end_version = version.end_version

                batch_start = processed_versions[0].start_version
                batch_end = processed_versions[-1].end_version

                batch_start_version = batch_end + 1

                # TODO: Update latest processed version metric
                self.processor.update_last_processed_version(batch_end)
                PROCESSED_TRANSACTIONS_COUNTER.inc(len(processed_versions))

                perf_end_time = perf_counter()

                print(
                    json.dumps(
                        {
                            "message": f"[Parser] Processed transactions",
                            "processor_name": self.processor.name(),
                            "start_version": str(batch_start),
                            "end_version": str(batch_end),
                            "batch_size": str(batch_end - batch_start + 1),
                            "tps": f"{(batch_end - batch_start + 1) / (perf_end_time - perf_start_time)}",
                        }
                    ),
                    flush=True,
                )

                # Stop processing if reached ending version
                if batch_end == ending_version:
                    print(
                        json.dumps(
                            {
                                "message": f"[Parser] Reached ending version {ending_version}. Exiting..."
                            }
                        ),
                        flush=True,
                    )
                    break

    def init_db_tables(self, schema_name: str) -> None:
        engine = create_engine(self.config.db_connection_uri)
        engine = engine.execution_options(
            schema_translate_map={"per_schema": schema_name}
        )
        Session.configure(bind=engine)
        Base.metadata.create_all(engine, checkfirst=True)

    def start_health_and_monitoring_ports(self) -> None:
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
