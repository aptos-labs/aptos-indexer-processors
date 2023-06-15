import datetime
import re

from aptos.transaction.v1 import transaction_pb2
from dataclasses import dataclass
from typing import List
from processors.nft_orderbooks.nft_marketplace_constants import (
    MARKETPLACE_SMART_CONTRACT_ADDRESSES,
    MARKETPLACE_ENTRY_FUNCTIONS,
    MARKETPLACE_ADDRESS_MATCH_REGEX_STRINGS,
)
from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName
from utils import general_utils, transaction_utils


@dataclass
class RawMarketplaceEvent:
    transaction_version: int
    event_index: int
    event_type: str
    json_data: str
    contract_address: str
    entry_function_name: str
    transaction_timestamp: datetime.datetime


def get_marketplace_events(
    transaction: transaction_pb2.Transaction, marketplaceName: MarketplaceName
) -> List[RawMarketplaceEvent]:
    # Filter out all non-user transactions
    if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
        return []

    transaction_version = transaction.version
    transaction_timestamp = general_utils.convert_pb_timestamp_to_datetime(
        transaction.timestamp
    )

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction is not None

    entry_function_payload = transaction_utils.get_entry_function_payload(
        user_transaction
    )
    entry_function = entry_function_payload.function
    module = entry_function.module
    contract_addr = module.address
    entry_function_name = f"{module.name}::{entry_function.name}"

    raw_marketplace_events = []
    for event_index, event in enumerate(user_transaction.events):
        event_type = event.type_str
        event_type_match = re.search(
            MARKETPLACE_ADDRESS_MATCH_REGEX_STRINGS[marketplaceName], event_type
        )
        if event_type_match == None:
            continue

        raw_marketplace_event = RawMarketplaceEvent(
            transaction_version,
            event_index,
            event_type,
            json_data=event.data,
            contract_address=contract_addr,
            entry_function_name=entry_function_name,
            transaction_timestamp=transaction_timestamp,
        )

        raw_marketplace_events.append(raw_marketplace_event)

    return raw_marketplace_events
