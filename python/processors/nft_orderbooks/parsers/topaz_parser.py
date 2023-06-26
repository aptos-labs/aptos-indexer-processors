import json

from aptos.transaction.v1 import transaction_pb2
from typing import List, Optional, Tuple
from processors.nft_orderbooks.nft_marketplace_constants import (
    TOPAZ_BIDS_TABLE_HANDLE,
    TOPAZ_COLLECTION_BIDS_TABLE_HANDLE,
    TOPAZ_LISTINGS_TABLE_HANDLE,
    TOPAZ_MARKETPLACE_EVENT_TYPES,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    BidMetadata,
    MarketplaceName,
    StandardMarketplaceEventType,
    ListingTableMetadata,
    MarketplaceEventMetadata,
    CollectionBidMetadata,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    parse_transaction_metadata,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from processors.nft_orderbooks.models.nft_marketplace_bid_models import (
    CurrentNFTMarketplaceBid,
    NFTMarketplaceBid,
    CurrentNFTMarketplaceCollectionBid,
    NFTMarketplaceCollectionBid,
)
from processors.nft_orderbooks.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from utils.token_utils import CollectionDataIdType, TokenDataIdType, standardize_address
from utils import event_utils, general_utils, transaction_utils, write_set_change_utils


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


def parse_event(
    transaction: transaction_pb2.Transaction,
    event: transaction_pb2.Event,
    event_index: int,
) -> List[
    NFTMarketplaceEvent
    | NFTMarketplaceListing
    | CurrentNFTMarketplaceListing
    | NFTMarketplaceBid
    | CurrentNFTMarketplaceBid
    | NFTMarketplaceCollectionBid
    | CurrentNFTMarketplaceCollectionBid
]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)

    # Filter out events we don't care about
    display_event_type = event_utils.get_event_type_short(event)

    if display_event_type not in TOPAZ_MARKETPLACE_EVENT_TYPES:
        return []

    event_metadata = parse_marketplace_event_metadata(event)
    standard_event_type = standardize_marketplace_event_type(display_event_type)

    activity = NFTMarketplaceEvent(
        transaction_version=transaction_metadata.transaction_version,
        event_index=event_index,
        event_type=display_event_type,
        standard_event_type=standard_event_type.value,
        creator_address=event_metadata.creator_address,
        collection=event_metadata.collection,
        token_name=event_metadata.token_name,
        token_data_id=event_metadata.token_data_id,
        collection_id=event_metadata.collection_id,
        price=event_metadata.price,
        token_amount=event_metadata.amount,
        buyer=event_metadata.buyer,
        seller=event_metadata.seller,
        json_data=event.data,
        marketplace=MarketplaceName.TOPAZ.value,
        contract_address=transaction_metadata.contract_address,
        entry_function_id_str=transaction_metadata.entry_function_id_str_short,
        transaction_timestamp=transaction_metadata.transaction_timestamp,
    )

    parsed_objs.append(activity)

    # Handle listing cancel and listing fill events
    if display_event_type in set(["events::DelistEvent", "events::BuyEvent"]):
        listing = NFTMarketplaceListing(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=event_metadata.creator_address,
            token_name=event_metadata.token_name,
            token_data_id=event_metadata.token_data_id,
            collection=event_metadata.collection,
            collection_id=event_metadata.collection_id,
            price=event_metadata.price,
            token_amount=event_metadata.amount * -1 if event_metadata.amount else None,
            buyer=event_metadata.buyer,
            seller=event_metadata.seller,
            event_type=standard_event_type.value,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_listing = CurrentNFTMarketplaceListing(
            token_data_id=event_metadata.token_data_id,
            creator_address=event_metadata.creator_address,
            token_name=event_metadata.token_name,
            collection=event_metadata.collection,
            collection_id=event_metadata.collection_id,
            price=event_metadata.price,
            token_amount=0,
            seller=event_metadata.seller,
            is_deleted=True,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        parsed_objs.append(listing)
        parsed_objs.append(current_listing)
    # Handle cancel bid event
    elif display_event_type == "events::CancelBidEvent":
        bid = NFTMarketplaceBid(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=event_metadata.creator_address,
            token_name=event_metadata.token_name,
            token_data_id=event_metadata.token_data_id,
            collection=event_metadata.collection,
            collection_id=event_metadata.collection_id,
            price=event_metadata.price,
            token_amount=event_metadata.amount * -1 if event_metadata.amount else None,
            seller=event_metadata.seller,
            buyer=event_metadata.buyer,
            event_type=standard_event_type.value,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_bid = CurrentNFTMarketplaceBid(
            token_data_id=event_metadata.token_data_id,
            creator_address=event_metadata.creator_address,
            token_name=event_metadata.token_name,
            collection=event_metadata.collection,
            collection_id=event_metadata.collection_id,
            price=event_metadata.price,
            token_amount=0,
            buyer=event_metadata.buyer,
            is_deleted=True,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(bid)
        parsed_objs.append(current_bid)

    return parsed_objs


def parse_write_table_item(
    transaction: transaction_pb2.Transaction,
    write_table_item: transaction_pb2.WriteTableItem,
    wsc_index: int,
) -> List[
    NFTMarketplaceEvent
    | NFTMarketplaceListing
    | CurrentNFTMarketplaceListing
    | NFTMarketplaceBid
    | CurrentNFTMarketplaceBid
    | NFTMarketplaceCollectionBid
    | CurrentNFTMarketplaceCollectionBid
]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)

    table_handle = write_table_item.handle
    # Handle listing place events
    if table_handle == TOPAZ_LISTINGS_TABLE_HANDLE:
        listing_data = parse_place_listing(write_table_item)
        listing = NFTMarketplaceListing(
            transaction_version=transaction_metadata.transaction_version,
            index=wsc_index,
            creator_address=listing_data.creator_address,
            token_name=listing_data.token_name,
            token_data_id=listing_data.token_data_id,
            collection=listing_data.collection,
            collection_id=listing_data.collection_id,
            price=listing_data.price,
            token_amount=listing_data.amount,
            seller=listing_data.seller,
            event_type=StandardMarketplaceEventType.LISTING_PLACE.value,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_listing = CurrentNFTMarketplaceListing(
            token_data_id=listing_data.token_data_id,
            creator_address=listing_data.creator_address,
            token_name=listing_data.token_name,
            collection=listing_data.collection,
            collection_id=listing_data.collection_id,
            price=listing_data.price,
            token_amount=listing_data.amount,
            seller=listing_data.seller,
            is_deleted=False,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(listing)
        parsed_objs.append(current_listing)

    # Handle bid place and bid filled events
    elif table_handle == TOPAZ_BIDS_TABLE_HANDLE:
        bid_data = parse_bid(write_table_item)

        bid_amount = 0
        is_deleted = False
        if bid_data.amount != None:
            if bid_data.amount > 0:
                bid_amount = bid_data.amount
            elif bid_data.amount == 0:
                bid_amount = -1  # Bid is deleted when filled
                is_deleted = True

        event_type = (
            StandardMarketplaceEventType.BID_FILLED
            if is_deleted
            else StandardMarketplaceEventType.BID_PLACE
        )

        seller = None
        if event_type == StandardMarketplaceEventType.BID_FILLED:
            seller = transaction_utils.get_sender(user_transaction)

        bid = NFTMarketplaceBid(
            transaction_version=transaction_metadata.transaction_version,
            index=wsc_index,
            creator_address=bid_data.creator_address,
            token_name=bid_data.token_name,
            token_data_id=bid_data.token_data_id,
            collection=bid_data.collection,
            collection_id=bid_data.collection_id,
            price=bid_data.price,
            token_amount=bid_amount,
            buyer=bid_data.buyer,
            seller=seller,
            event_type=event_type.value,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_bid = CurrentNFTMarketplaceBid(
            token_data_id=bid_data.token_data_id,
            creator_address=bid_data.creator_address,
            token_name=bid_data.token_name,
            collection=bid_data.collection,
            collection_id=bid_data.collection_id,
            price=bid_data.price,
            token_amount=bid_data.amount,
            buyer=bid_data.buyer,
            is_deleted=is_deleted,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(bid)
        parsed_objs.append(current_bid)

    # Handle collection bid place and cancel and fill
    elif table_handle == TOPAZ_COLLECTION_BIDS_TABLE_HANDLE:
        collection_bid_data = parse_collection_bid(write_table_item)

        # Count the number of tokens filled
        num_filled = 0
        for event in user_transaction.events:
            if "events::FillCollectionBidEvent" in event.type_str:
                event_metadata = parse_marketplace_event_metadata(event)
                num_filled += event_metadata.amount or 0

        amount = collection_bid_data.amount
        current_amount = collection_bid_data.amount
        seller = None
        is_deleted = False
        if collection_bid_data.is_cancelled:
            event_type = StandardMarketplaceEventType.BID_CANCEL
            amount = -1 * amount if amount else None
            current_amount = 0
            is_deleted = True
        elif num_filled > 0:
            event_type = StandardMarketplaceEventType.BID_FILLED
            amount = -1 * num_filled
            seller = transaction_utils.get_sender(user_transaction)
        else:
            event_type = StandardMarketplaceEventType.BID_PLACE

        collection_bid = NFTMarketplaceCollectionBid(
            transaction_version=transaction_metadata.transaction_version,
            index=wsc_index,
            creator_address=collection_bid_data.creator_address,
            collection=collection_bid_data.collection,
            collection_id=collection_bid_data.collection_id,
            price=collection_bid_data.price,
            token_amount=amount,
            buyer=collection_bid_data.buyer,
            seller=seller,
            event_type=event_type.value,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_collection_bid = CurrentNFTMarketplaceCollectionBid(
            creator_address=collection_bid_data.creator_address,
            collection=collection_bid_data.collection,
            collection_id=collection_bid_data.collection_id,
            price=collection_bid_data.price,
            token_amount=current_amount,
            buyer=collection_bid_data.buyer,
            is_deleted=is_deleted,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(collection_bid)
        parsed_objs.append(current_collection_bid)

    return parsed_objs


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


def parse_bid(write_table_item: transaction_pb2.WriteTableItem) -> BidMetadata:
    data = json.loads(write_table_item.data.value)

    # Collection, token, and creator parsing
    token_data_id_struct = data.get("token_id", {}).get("token_data_id", {})
    collection = token_data_id_struct.get("collection", None)
    token_name = token_data_id_struct.get("name", None)
    creator = token_data_id_struct.get("creator", None)

    collection_data_id_type = CollectionDataIdType(creator, collection)
    token_data_id_type = TokenDataIdType(creator, collection, token_name)
    token_name_trunc = token_data_id_type.get_name_trunc()
    token_data_id = token_data_id_type.to_hash()

    # Price parsing
    price = data.get("price") or data.get("min_price") or data.get("coin_amount")
    price = int(price) if price else None

    # Amount parsing
    amount = data.get("amount") or data.get("token_amount")
    amount = int(amount) if amount else None

    # Buyer and seller parsing
    buyer = data.get("buyer", None) or data.get("token_buyer", None)

    return BidMetadata(
        creator_address=standardize_address(creator),
        collection=collection_data_id_type.get_name_trunc(),
        token_name=token_name_trunc,
        token_data_id=token_data_id,
        collection_id=collection_data_id_type.to_hash(),
        price=price,
        amount=amount,
        buyer=standardize_address(buyer) if buyer != None else None,
        seller=None,  # This is parsed from the transaction
    )


def parse_collection_bid(
    write_table_item: transaction_pb2.WriteTableItem,
) -> CollectionBidMetadata:
    data = json.loads(write_table_item.data.value)

    # Collection creator parsing
    collection = data.get("collection_name", None)
    creator = data.get("creator", None)

    collection_data_id_type = CollectionDataIdType(creator, collection)

    # Price parsing
    price = data.get("price")
    price = int(price) if price else None

    # Amount parsing
    amount = data.get("amount")
    amount = int(amount) if amount else None

    # Buyer parsing
    buyer = data.get("buyer", None)

    cancelled = data.get("cancelled", None)
    cancelled = bool(cancelled) if cancelled != None else None

    return CollectionBidMetadata(
        creator_address=standardize_address(creator),
        collection=collection_data_id_type.get_name_trunc(),
        collection_id=collection_data_id_type.to_hash(),
        price=price,
        amount=amount,
        buyer=standardize_address(buyer) if buyer != None else None,
        seller=None,  # This is parsed from the transaction
        is_cancelled=cancelled,
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
