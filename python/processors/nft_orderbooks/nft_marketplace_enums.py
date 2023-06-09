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


class StandardMarketplaceEventType(Enum):
    BID_CANCEL = "bid cancel"
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
    price: Optional[int]
    amount: Optional[int]
    seller: Optional[str]


@dataclass
class MarketplaceEventMetadata:
    creator_address: Optional[str]
    token_data_id: Optional[str]
    token_name: Optional[str]
    collection: Optional[str]
    collection_id: Optional[str]
    price: Optional[int]
    amount: Optional[int]
    buyer: Optional[str]
    seller: Optional[str]


@dataclass
class BidMetadata:
    creator_address: Optional[str]
    token_data_id: Optional[str]
    token_name: Optional[str]
    collection: Optional[str]
    collection_id: Optional[str]
    price: Optional[int]
    amount: Optional[int]
    buyer: Optional[str]


@dataclass
class CollectionBidMetadata:
    creator_address: Optional[str]
    collection: Optional[str]
    collection_id: Optional[str]
    price: Optional[int]
    amount: Optional[int]
    buyer: Optional[str]
    is_deleted: Optional[bool]
