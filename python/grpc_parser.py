from aptos.util.timestamp import timestamp_pb2
from aptos.transaction.testing1.v1 import transaction_pb2
import datetime

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
    for event_index, event in enumerate(user_transaction.events):
        creation_number = event.key.creation_number
        sequence_number = event.sequence_number
        account_address = event.key.account_address
        type = event.type_str
        data = event.data

        # Insert transaction into database
        print(transaction_version)
        insert_into_db(
            creation_number, 
            sequence_number, 
            account_address, 
            type, 
            transaction_version, 
            transaction_block_height, 
            data, 
            inserted_at, 
            event_index,
        )

def parse_timestamp(timestamp: timestamp_pb2.Timestamp):
    datetime_obj = datetime.datetime.fromtimestamp(timestamp.seconds + timestamp.nanos * 1e-9)
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S.%f')

def insert_into_db(
    creation_number, 
    sequence_number, 
    account_address, 
    type, 
    transaction_version, 
    transaction_block_height, 
    data, 
    inserted_at, 
    event_index,
):
    # Implement me! :)
    pass
