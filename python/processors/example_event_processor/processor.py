from aptos_protos.aptos.transaction.v1 import transaction_pb2
from processors.example_event_processor.models import Event
from typing import List
from utils.transactions_processor import ProcessingResult
from utils import general_utils
from utils.transactions_processor import TransactionsProcessor
from utils.models.schema_names import EXAMPLE
from utils.session import Session
from utils.processor_name import ProcessorName
from time import perf_counter


class ExampleEventProcessor(TransactionsProcessor):
    def name(self) -> str:
        return ProcessorName.EXAMPLE_EVENT_PROCESSOR.value

    def schema(self) -> str:
        return EXAMPLE

    def process_transactions(
        self,
        transactions: list[transaction_pb2.Transaction],
        start_version: int,
        end_version: int,
    ) -> ProcessingResult:
        event_db_objs: List[Event] = []
        start_time = perf_counter()
        for transaction in transactions:
            # Custom filtering
            # Here we filter out all transactions that are not of type TRANSACTION_TYPE_USER
            if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
                continue

            # Parse Transaction struct
            transaction_version = transaction.version
            transaction_block_height = transaction.block_height
            transaction_timestamp = general_utils.parse_pb_timestamp(
                transaction.timestamp
            )
            user_transaction = transaction.user

            # Parse Event struct
            for event_index, event in enumerate(user_transaction.events):
                creation_number = event.key.creation_number
                sequence_number = event.sequence_number
                account_address = general_utils.standardize_address(
                    event.key.account_address
                )
                type = event.type_str
                data = event.data

                # Create an instance of Event
                event_db_obj = Event(
                    creation_number=creation_number,
                    sequence_number=sequence_number,
                    account_address=account_address,
                    transaction_version=transaction_version,
                    transaction_block_height=transaction_block_height,
                    type=type,
                    data=data,
                    transaction_timestamp=transaction_timestamp,
                    event_index=event_index,
                )
                event_db_objs.append(event_db_obj)
        processing_duration_in_secs = perf_counter() - start_time
        start_time = perf_counter()
        self.insert_to_db(event_db_objs)
        db_insertion_duration_in_secs = perf_counter() - start_time
        return ProcessingResult(
            start_version=start_version,
            end_version=end_version,
            processing_duration_in_secs=processing_duration_in_secs,
            db_insertion_duration_in_secs=db_insertion_duration_in_secs,
        )

    def insert_to_db(self, parsed_objs: List[Event]) -> None:
        with Session() as session, session.begin():
            for obj in parsed_objs:
                session.merge(obj)
