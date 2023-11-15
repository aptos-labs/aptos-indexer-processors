from aptos_protos.aptos.transaction.v1 import transaction_pb2
from processors.coin_flip.models import CoinFlipEvent
from typing import List
from utils.transactions_processor import ProcessingResult
from utils import general_utils
from utils.transactions_processor import TransactionsProcessor
from utils.models.schema_names import COIN_FLIP_SCHEMA_NAME
from utils.session import Session
from utils.processor_name import ProcessorName
import json
from datetime import datetime
from time import perf_counter

MODULE_ADDRESS = general_utils.standardize_address(
    "0xe57752173bc7c57e9b61c84895a75e53cd7c0ef0855acd81d31cb39b0e87e1d0"
)


class CoinFlipProcessor(TransactionsProcessor):
    def name(self) -> str:
        return ProcessorName.COIN_FLIP.value

    def schema(self) -> str:
        return COIN_FLIP_SCHEMA_NAME

    def process_transactions(
        self,
        transactions: list[transaction_pb2.Transaction],
        start_version: int,
        end_version: int,
    ) -> ProcessingResult:
        event_db_objs: List[CoinFlipEvent] = []
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

            # Parse CoinFlipEvent struct
            for event_index, event in enumerate(user_transaction.events):
                # Skip events that don't match our filter criteria
                if not CoinFlipProcessor.included_event_type(event.type_str):
                    continue

                creation_number = event.key.creation_number
                sequence_number = event.sequence_number
                account_address = general_utils.standardize_address(
                    event.key.account_address
                )

                # Convert your on-chain data scheme to database-friendly values
                # Our on-chain struct looks like this:
                #   struct CoinFlipEvent has copy, drop, store {
                #       prediction: bool,
                #       result: bool,
                #       timestamp: u64,
                #   }
                # These values are stored in the `data` field of the event as JSON fields/values
                # Load the data into a json object and then use it as a regular dictionary
                data = json.loads(event.data)
                prediction = bool(data["prediction"])
                result = bool(data["result"])
                wins = int(data["wins"])
                losses = int(data["losses"])

                # We have extra data to insert into the database, because we want to process our data.
                # Calculate the total
                win_percentage = wins / (wins + losses)

                # Create an instance of CoinFlipEvent
                event_db_obj = CoinFlipEvent(
                    sequence_number=sequence_number,
                    creation_number=creation_number,
                    account_address=account_address,
                    transaction_version=transaction_version,
                    transaction_timestamp=transaction_timestamp,
                    losses=losses,
                    prediction=prediction,
                    result=result,
                    wins=wins,
                    win_percentage=win_percentage,
                    event_index=event_index,  # when multiple events of the same type are emitted in a single transaction, this is the index of the event in the transaction
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

    def insert_to_db(self, parsed_objs: List[CoinFlipEvent]) -> None:
        with Session() as session, session.begin():
            for obj in parsed_objs:
                session.merge(obj)

    @staticmethod
    def included_event_type(event_type: str) -> bool:
        parsed_tag = event_type.split("::")
        module_address = general_utils.standardize_address(parsed_tag[0])
        module_name = parsed_tag[1]
        event_type = parsed_tag[2]
        # Now we can filter out events that are not of type CoinFlipEvent
        # We can filter by the module address, module name, and event type
        # If someone deploys a different version of our contract with the same event type, we may want to index it one day.
        # So we could only check the event type instead of the full string
        # For our sake, check the full string
        return (
            module_address == MODULE_ADDRESS
            and module_name == "coin_flip"
            and event_type == "CoinFlipEvent"
        )
