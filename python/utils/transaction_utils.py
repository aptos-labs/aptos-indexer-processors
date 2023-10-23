from aptos_protos.aptos.transaction.v1 import transaction_pb2
from typing import List, Optional

# Utility functions for transaction_pb2


def get_user_transaction(
    transaction: transaction_pb2.Transaction,
) -> Optional[transaction_pb2.UserTransaction]:
    if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
        return None
    return transaction.user


def get_user_transaction_request(
    user_transaction: transaction_pb2.UserTransaction,
) -> transaction_pb2.UserTransactionRequest:
    return user_transaction.request


def get_transaction_payload(
    user_transaction: transaction_pb2.UserTransaction,
) -> transaction_pb2.TransactionPayload:
    user_transaction_request = get_user_transaction_request(user_transaction)
    return user_transaction_request.payload


def get_sender(
    user_transaction: transaction_pb2.UserTransaction,
) -> str:
    user_transaction_request = get_user_transaction_request(user_transaction)
    return user_transaction_request.sender


def get_entry_function_payload(
    user_transaction: transaction_pb2.UserTransaction,
) -> transaction_pb2.EntryFunctionPayload:
    transaction_payload = get_transaction_payload(user_transaction)
    return transaction_payload.entry_function_payload


def get_entry_function_id_str_short(
    user_transaction: transaction_pb2.UserTransaction,
) -> str:
    entry_function_payload = get_entry_function_payload(user_transaction)
    entry_function = entry_function_payload.function
    module = entry_function.module
    entry_function_name = f"{module.name}::{entry_function.name}"
    return entry_function_name


def get_move_module(
    user_transaction: transaction_pb2.UserTransaction,
) -> transaction_pb2.MoveModuleId:
    entry_function_payload = get_entry_function_payload(user_transaction)
    return entry_function_payload.function.module


def get_contract_address(
    user_transaction: transaction_pb2.UserTransaction,
) -> str:
    move_module = get_move_module(user_transaction)
    return move_module.address


def get_write_set_changes(
    transaction: transaction_pb2.Transaction,
) -> List[transaction_pb2.WriteSetChange]:
    transaction_info = transaction.info
    return list(transaction_info.changes)


def get_move_type_str(
    move_type: transaction_pb2.MoveType,
) -> str:
    struct = move_type.struct
    return f"{struct.address}::{struct.module}::{struct.name}"
