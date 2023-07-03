import datetime

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class MarketplaceName(Enum):
    TOPAZ = "topaz"
    SOUFFLE = "souffle"
    BLUEMOVE = "bluemove"
    OKX = "okx"
    OZOZOZ = "ozozoz"
    ITSRARE = "itsrare"
    APTOMINGOS_AUCTION = "aptomingos_auction"
    EXAMPLE_V2_MARKETPLACE = "example_v2_marketplace"


class StandardMarketplaceEventType(Enum):
    BID_CANCEL = "bid cancel"
    BID_CHANGE = "bid change"
    BID_FILLED = "bid filled"
    BID_PLACE = "bid place"
    LISTING_CANCEL = "listing cancel"
    LISTING_CHANGE = "listing change"
    LISTING_FILLED = "listing filled"
    LISTING_PLACE = "listing place"
    UNKNOWN = "unknown"


@dataclass
class ListingTableMetadata:
    creator_address: Optional[str]
    token_data_id: Optional[str]
    token_name: Optional[str]
    collection: Optional[str]
    collection_id: Optional[str]
    price: Optional[float]
    amount: Optional[float]
    seller: Optional[str]


@dataclass
class MarketplaceEventMetadata:
    creator_address: Optional[str]
    token_data_id: Optional[str]
    token_name: Optional[str]
    collection: Optional[str]
    collection_id: Optional[str]
    price: Optional[float]
    amount: Optional[float]
    buyer: Optional[str]
    seller: Optional[str]


@dataclass
class BidMetadata:
    creator_address: Optional[str]
    token_data_id: Optional[str]
    token_name: Optional[str]
    collection: Optional[str]
    collection_id: Optional[str]
    price: Optional[float]
    amount: Optional[float]
    buyer: Optional[str]
    seller: Optional[str]


@dataclass
class CollectionBidMetadata:
    creator_address: Optional[str]
    collection: Optional[str]
    collection_id: Optional[str]
    price: Optional[float]
    amount: Optional[float]  # Amount of tokens left in the collection bid
    buyer: Optional[str]
    seller: Optional[str]
    is_cancelled: Optional[bool]


@dataclass
class TransactionMetadata:
    transaction_version: int
    transaction_timestamp: datetime.datetime
    contract_address: str
    entry_function_id_str_short: str
