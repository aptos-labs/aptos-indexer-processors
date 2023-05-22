from aptos.transaction.testing1.v1.transaction_pb2 import Event

def get_account_address(event: Event) -> str:
    return event.key.account_address