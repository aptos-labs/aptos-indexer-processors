import argparse
import grpc
import json

from dataclasses import dataclass
from utils.models.general_models import NextVersionToProcess
from aptos_protos.aptos.transaction.v1 import transaction_pb2
from utils.config import Config
from utils.models.general_models import Base
from utils.session import Session
from abc import ABC, abstractmethod
from sqlalchemy.dialects.postgresql import insert


@dataclass
class ProcessingResult:
    start_version: int
    end_version: int
    processing_duration_in_secs: float
    db_insertion_duration_in_secs: float


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
            insert_stmt = insert(NextVersionToProcess).values(
                indexer_name=self.name(), next_version=last_processed_version + 1
            )
            on_conflict_do_update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["indexer_name"],
                set_=dict(insert_stmt.excluded.items()),
                where=(
                    insert_stmt.excluded["next_version"]
                    > NextVersionToProcess.next_version
                ),
            )
            session.execute(on_conflict_do_update_stmt)
