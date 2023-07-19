import argparse
import grpc
import json

from dataclasses import dataclass
from utils.models.general_models import NextVersionToProcess
from aptos.indexer.v1 import raw_data_pb2, raw_data_pb2_grpc
from aptos.transaction.v1 import transaction_pb2
from utils.config import Config
from utils.models.general_models import Base
from utils.session import Session
from utils.metrics import PROCESSED_TRANSACTIONS_COUNTER
from sqlalchemy import DDL, Engine, create_engine
from sqlalchemy import event
from typing import Any, Callable, TypedDict
from prometheus_client import start_http_server
import http.server
import socketserver
import threading
import sys
import traceback
from abc import ABC, abstractmethod


@dataclass
class ProcessingResult:
    start_version: int
    end_version: int


class TransactionsProcessor(ABC):
    config: Config
    num_concurrent_processing_tasks: int

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
    ) -> ProcessingResult:
        pass

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
