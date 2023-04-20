from aptos.util.timestamp import timestamp_pb2
from aptos.transaction.testing1.v1 import transaction_pb2
from create_table import Event
import datetime

# INDEXER_NAME is used to track the latest processed version
INDEXER_NAME = "python_example_indexer"


def parse(transaction: transaction_pb2.Transaction):
    # Custom filtering
    # Here we filter out all transactions that are not of type TRANSACTION_TYPE_USER
    if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
        return

    # Parse Transaction struct
    transaction_version = transaction.version
    transaction_block_height = transaction.block_height
    inserted_at = parse_timestamp(transaction.timestamp)
    user_transaction = transaction.user

    # Parse Event struct
    event_db_objs: list[Event] = []
    for event_index, event in enumerate(user_transaction.events):
        creation_number = event.key.creation_number
        sequence_number = event.sequence_number
        account_address = standardize_address(event.key.account_address)
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
            inserted_at=inserted_at,
            event_index=event_index,
        )
        event_db_objs.append(event_db_obj)

    return event_db_objs


def parse_timestamp(timestamp: timestamp_pb2.Timestamp):
    datetime_obj = datetime.datetime.fromtimestamp(
        timestamp.seconds + timestamp.nanos * 1e-9
    )
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S.%f")


def standardize_address(address: str):
    return "0x" + address
