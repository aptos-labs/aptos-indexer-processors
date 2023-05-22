from aptos.transaction.testing1.v1 import transaction_pb2
from typing import Optional

# Utility functions for transaction_pb2

def get_user_transaction(transaction: transaction_pb2.Transaction) -> Optional[transaction_pb2.UserTransaction]:
    if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
        return None
    return transaction.user

def get_user_transaction_request(user_transaction: transaction_pb2.UserTransaction) -> transaction_pb2.UserTransactionRequest:
    return user_transaction.request

def get_transaction_payload(user_transaction: transaction_pb2.UserTransaction) -> transaction_pb2.TransactionPayload:
    user_transaction_request = get_user_transaction_request(user_transaction)
    return user_transaction_request.payload

def get_entry_function_payload(user_transaction: transaction_pb2.UserTransaction) -> transaction_pb2.EntryFunctionPayload:
    transaction_payload = get_transaction_payload(user_transaction)
    return transaction_payload.entry_function_payload

def get_move_module(user_transaction: transaction_pb2.UserTransaction) -> transaction_pb2.MoveModuleId:
    entry_function_payload = get_entry_function_payload(user_transaction)
    return entry_function_payload.function.module