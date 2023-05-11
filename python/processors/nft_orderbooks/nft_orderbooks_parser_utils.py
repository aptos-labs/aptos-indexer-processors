import re

from aptos.transaction.testing1.v1 import transaction_pb2
from aptos.util.timestamp import timestamp_pb2
from enum import Enum
from dataclasses import dataclass
from typing import List


class MarketplaceName(Enum):
    TOPAZ = "topaz"
    SOUFFLE = "souffle"
    BLUEMOVE = "bluemove"
    OKX = "okx"
    OZOZOZ = "ozozoz"
    ITSRARE = "itsrare"
    APTOMINGOS_AUCTION = "aptomingos_auction"


class StandardMarketplaceEventType(Enum):
    BID_CANCEL = "bid cancel"
    BID_FILLED = "bid filled"
    BID_PLACE = "bid place"
    LISTING_CANCEL = "listing cancel"
    LISTING_CHANGE = "listing change"
    LISTING_FILLED = "listing filled"
    LISTING_PLACE = "listing place"
    UNKNOWN = "unknown"


MARKETPLACE_ADDRESS_MATCH_REGEX_STRINGS = {
    MarketplaceName.TOPAZ: "^0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2.*$",
    MarketplaceName.SOUFFLE: "^0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4.*FixedPriceMarket.*$",
    MarketplaceName.BLUEMOVE: "^0xd1fd99c1944b84d1670a2536417e997864ad12303d19eac725891691b04d614e.*$",
    MarketplaceName.OKX: "^0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43.*$",
    MarketplaceName.OZOZOZ: "^0xded0c1249b522cecb11276d2fad03e6635507438fef042abeea3097846090bcd.*OzozozMarketplace.*$",
    MarketplaceName.ITSRARE: "^0x143f6a7a07c76eae1fc9ea030dbd9be3c2d46e538c7ad61fe64529218ff44bc4.*$",
    MarketplaceName.APTOMINGOS_AUCTION: "^0x98937acca8bc2c164dff158156ab06f9c99bbbb050129d7a514a37ccb1b8e49e.*$",
}


@dataclass
class RawMarketplaceEvent:
    transaction_version: int
    event_index: int
    event_type: str
    json_data: str
    contract_address: str
    entry_function_name: str
    transaction_timestamp: int


def get_marketplace_events(
    transaction: transaction_pb2.Transaction, marketplaceName: MarketplaceName
) -> List[RawMarketplaceEvent]:
    # Filter out all non-user transactions
    if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
        return []

    transaction_version = transaction.version
    transaction_timestamp = convert_timestamp_to_int64(transaction.timestamp)

    user_transaction = transaction.user
    user_transaction_request = user_transaction.request
    transaction_payload = user_transaction_request.payload
    entry_function_payload = transaction_payload.entry_function_payload
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


def convert_timestamp_to_int64(timestamp: timestamp_pb2.Timestamp) -> int:
    return timestamp.seconds * 1000000 + int(timestamp.nanos / 1000)
