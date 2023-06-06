from aptos.transaction.v1 import transaction_pb2
from processors.example_event_processor.models import Event
from typing import List

from utils import general_utils
from utils.transactions_processor import TransactionsProcessor
from utils.models.general_models import NextVersionToProcess, Base


# Define the parser function that will be used by the TransactionProcessor
def parse(transaction: transaction_pb2.Transaction) -> List[Event]:
    # Custom filtering
    # Here we filter out all transactions that are not of type TRANSACTION_TYPE_USER
    if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
        return []

    # Parse Transaction struct
    transaction_version = transaction.version
    transaction_block_height = transaction.block_height
    transaction_timestamp = general_utils.parse_pb_timestamp(transaction.timestamp)
    user_transaction = transaction.user

    # Parse Event struct
    event_db_objs: list[Event] = []
    for event_index, event in enumerate(user_transaction.events):
        creation_number = event.key.creation_number
        sequence_number = event.sequence_number
        account_address = general_utils.standardize_address(event.key.account_address)
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

    return event_db_objs


if __name__ == "__main__":
    transactions_processor = TransactionsProcessor(
        parse,
    )
    transactions_processor.process()
