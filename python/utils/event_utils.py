from aptos_protos.aptos.transaction.v1.transaction_pb2 import Event


def get_account_address(event: Event) -> str:
    return event.key.account_address


def get_event_type_address(event: Event) -> str:
    type_strings = event.type_str.split("::")
    return type_strings[0]


def get_event_type_short(event: Event) -> str:
    type_strings = event.type_str.split("::")
    return "::".join(type_strings[1:])
