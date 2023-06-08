import json

from aptos.transaction.v1 import transaction_pb2
from dataclasses import dataclass
from typing import List, Tuple
from processors.nft_orderbooks.nft_marketplace_constants import (
    TOPAZ_LISTINGS_TABLE_HANDLE,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    MarketplaceName,
    StandardMarketplaceEventType,
    ListingTableMetadata,
    MarketplaceEventMetadata,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from processors.nft_orderbooks.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from utils.token_utils import CollectionDataIdType, TokenDataIdType, standardize_address
from utils import event_utils, general_utils, transaction_utils, write_set_change_utils

TOPAZ_MARKETPLACE_EVENT_TYPES = set(
    [
        # these are trade-related events
        # trade-related events are by nature also orderbook-related events
        "events::BuyEvent",
        "events::SellEvent",
        "events::FillCollectionBidEvent",
        # these are orderbook-related events
        "events::ListEvent",
        "events::DelistEvent",
        "events::BidEvent",
        "events::CancelBidEvent",
        "events::CollectionBidEvent",
        "events::CancelCollectionBidEvent"
        # unrelated to order book; these are filtered out (included here in comments for documentation)
        # "token_coin_swap::TokenListingEvent" -- redundant with events::ListEvent
        # "token_coin_swap::TokenSwapEvent"    -- redundant with events::BuyEvent
        # "events::SendEvent" -- transfer events do not affect order book
        # "events::ClaimEvent" -- transfer events do not affect order book
    ]
)


def standardize_marketplace_event_type(
    marketplace_event_type: str,
) -> StandardMarketplaceEventType:
    match marketplace_event_type:
        case "events::BuyEvent":
            return StandardMarketplaceEventType.LISTING_FILLED
        case "events::ListEvent":
            return StandardMarketplaceEventType.LISTING_PLACE
        case "events::DelistEvent":
            return StandardMarketplaceEventType.LISTING_CANCEL
        case "events::SellEvent" | "events::FillCollectionBidEvent":
            return StandardMarketplaceEventType.BID_FILLED
        case "events::BidEvent" | "events::CollectionBidEvent":
            return StandardMarketplaceEventType.BID_PLACE
        case "events::CancelBidEvent" | "events::CancelCollectionBidEvent":
            return StandardMarketplaceEventType.BID_CANCEL
        case _:
            return StandardMarketplaceEventType.UNKNOWN


def parse_transaction(
    transaction: transaction_pb2.Transaction,
) -> Tuple[
    List[NFTMarketplaceEvent],
    List[NFTMarketplaceListing],
    List[CurrentNFTMarketplaceListing],
]:
    nft_marketplace_activities: List[NFTMarketplaceEvent] = []
    nft_marketplace_listings: List[NFTMarketplaceListing] = []
    current_nft_marketplace_listings: List[CurrentNFTMarketplaceListing] = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_version = transaction.version
    transaction_timestamp = general_utils.convert_pb_timestamp_to_datetime(
        transaction.timestamp
    )
    contract_address = transaction_utils.get_contract_address(user_transaction)
    entry_function_id_str_short = transaction_utils.get_entry_function_id_str_short(
        user_transaction
    )

    # Parse events
    events = user_transaction.events
    for event_index, event in enumerate(events):
        # Filter out events we don't care about
        display_event_type = event_utils.get_event_type_short(event)

        if display_event_type not in TOPAZ_MARKETPLACE_EVENT_TYPES:
            continue

        event_metadata = parse_marketplace_event_metadata(event)

        activity = NFTMarketplaceEvent(
            transaction_version=transaction_version,
            event_index=event_index,
            event_type=display_event_type,
            standard_event_type=standardize_marketplace_event_type(
                display_event_type
            ).value,
            creator_address=event_metadata.creator_address,
            collection=event_metadata.collection,
            token_name=event_metadata.token_name,
            token_data_id=event_metadata.token_data_id,
            collection_id=event_metadata.collection_id,
            price=event_metadata.price,
            amount=event_metadata.amount,
            buyer=event_metadata.buyer,
            seller=event_metadata.seller,
            json_data=event.data,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=contract_address,
            entry_function_id_str=entry_function_id_str_short,
            transaction_timestamp=transaction_timestamp,
        )

        nft_marketplace_activities.append(activity)

        # Handle delist and list fill events
        if display_event_type in set(["events::DelistEvent", "events::BuyEvent"]):
            listing = NFTMarketplaceListing(
                transaction_version=transaction_version,
                index=event_index * -1,
                creator_address=event_metadata.creator_address,
                token_name=event_metadata.token_name,
                token_data_id=event_metadata.token_data_id,
                collection=event_metadata.collection,
                collection_id=event_metadata.collection_id,
                price=event_metadata.price,
                amount=event_metadata.amount * -1 if event_metadata.amount else None,
                seller=event_metadata.seller,
                marketplace=MarketplaceName.TOPAZ.value,
                contract_address=contract_address,
                entry_function_id_str=entry_function_id_str_short,
                transaction_timestamp=transaction_timestamp,
            )
            current_listing = CurrentNFTMarketplaceListing(
                token_data_id=event_metadata.token_data_id,
                creator_address=event_metadata.creator_address,
                token_name=event_metadata.token_name,
                collection=event_metadata.collection,
                collection_id=event_metadata.collection_id,
                price=event_metadata.price,
                amount=0,
                seller=event_metadata.seller,
                is_deleted=True,
                marketplace=MarketplaceName.TOPAZ.value,
                contract_address=contract_address,
                entry_function_id_str=entry_function_id_str_short,
                last_transaction_version=transaction_version,
                last_transaction_timestamp=transaction_timestamp,
            )

            nft_marketplace_listings.append(listing)
            current_nft_marketplace_listings.append(current_listing)

    # Parse write set changes for listings and bids
    write_set_changes = transaction_utils.get_write_set_changes(transaction)

    for wsc_index, wsc in enumerate(write_set_changes):
        write_table_item = write_set_change_utils.get_write_table_item(wsc)
        match (wsc.type):
            case transaction_pb2.WriteSetChange.TYPE_WRITE_TABLE_ITEM:
                write_table_item = write_set_change_utils.get_write_table_item(wsc)
                if not write_table_item:
                    continue

                table_handle = write_table_item.handle
                if table_handle == TOPAZ_LISTINGS_TABLE_HANDLE:
                    listing_data = parse_place_listing(write_table_item)
                    listing = NFTMarketplaceListing(
                        transaction_version=transaction_version,
                        index=wsc_index,
                        creator_address=listing_data.creator_address,
                        token_name=listing_data.token_name,
                        token_data_id=listing_data.token_data_id,
                        collection=listing_data.collection,
                        collection_id=listing_data.collection_id,
                        price=listing_data.price,
                        amount=listing_data.amount,
                        seller=listing_data.seller,
                        marketplace=MarketplaceName.TOPAZ.value,
                        contract_address=contract_address,
                        entry_function_id_str=entry_function_id_str_short,
                        transaction_timestamp=transaction_timestamp,
                    )
                    current_listing = CurrentNFTMarketplaceListing(
                        token_data_id=listing_data.token_data_id,
                        creator_address=listing_data.creator_address,
                        token_name=listing_data.token_name,
                        collection=listing_data.collection,
                        collection_id=listing_data.collection_id,
                        price=listing_data.price,
                        amount=listing_data.amount,
                        seller=listing_data.seller,
                        is_deleted=False,
                        marketplace=MarketplaceName.TOPAZ.value,
                        contract_address=contract_address,
                        entry_function_id_str=entry_function_id_str_short,
                        last_transaction_version=transaction_version,
                        last_transaction_timestamp=transaction_timestamp,
                    )
                    nft_marketplace_listings.append(listing)
                    current_nft_marketplace_listings.append(current_listing)

    return (
        nft_marketplace_activities,
        nft_marketplace_listings,
        current_nft_marketplace_listings,
    )


def parse_place_listing(
    write_table_item: transaction_pb2.WriteTableItem,
) -> ListingTableMetadata:
    table_data = json.loads(write_table_item.data.value)

    # Collection, token, and creator parsing
    token_data_id_struct = table_data.get("token_id", {}).get("token_data_id", {})
    collection = token_data_id_struct.get("collection", None)
    token_name = token_data_id_struct.get("name", None)
    creator = token_data_id_struct.get("creator", None)

    token_name_trunc = None
    token_data_id = None
    collection_data_id_type = CollectionDataIdType(creator, collection)
    if token_name != None:
        token_data_id_type = TokenDataIdType(creator, collection, token_name)
        token_name_trunc = token_data_id_type.get_name_trunc()
        token_data_id = token_data_id_type.to_hash()

    # Price parsing
    price = table_data.get("price", None)
    price = int(price) if price else None

    # Amount parsing
    amount = table_data.get("amount", None)
    amount = int(amount) if amount else None

    # Seller parsing
    seller = table_data.get("seller", None)
    seller = standardize_address(seller) if seller else None

    return ListingTableMetadata(
        creator_address=standardize_address(creator),
        token_name=token_name_trunc,
        token_data_id=token_data_id,
        collection=collection_data_id_type.get_name_trunc(),
        collection_id=collection_data_id_type.to_hash(),
        price=price,
        amount=amount,
        seller=seller,
    )


def parse_marketplace_event_metadata(
    event: transaction_pb2.Event,
) -> MarketplaceEventMetadata:
    data = json.loads(event.data)

    # Collection, token, and creator parsing
    token_data_id_struct = data.get("token_id", {}).get("token_data_id", {})
    collection = token_data_id_struct.get("collection", None) or data.get(
        "collection_name", None
    )
    token_name = token_data_id_struct.get("name", None)
    creator = token_data_id_struct.get("creator", None) or data.get("creator", None)

    token_name_trunc = None
    token_data_id = None
    collection_data_id_type = CollectionDataIdType(creator, collection)
    if token_name != None:
        token_data_id_type = TokenDataIdType(creator, collection, token_name)
        token_name_trunc = token_data_id_type.get_name_trunc()
        token_data_id = token_data_id_type.to_hash()

    # Price parsing
    price = int(
        data.get("price") or data.get("min_price") or data.get("coin_amount") or 0
    )

    # Amount parsing
    amount = int(data.get("amount") or data.get("token_amount") or 0)

    # Buyer and seller parsing
    buyer = data.get("buyer", None) or data.get("token_buyer", None)
    seller = data.get("seller", None)

    return MarketplaceEventMetadata(
        creator_address=standardize_address(creator),
        collection=collection_data_id_type.get_name_trunc(),
        token_name=token_name_trunc,
        token_data_id=token_data_id,
        collection_id=collection_data_id_type.to_hash(),
        price=price,
        amount=amount,
        buyer=standardize_address(buyer) if buyer != None else None,
        seller=standardize_address(seller) if seller != None else None,
    )
