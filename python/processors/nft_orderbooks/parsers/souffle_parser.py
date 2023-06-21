from aptos.transaction.v1 import transaction_pb2
import json
from typing import List
from processors.nft_orderbooks.nft_marketplace_constants import (
    SOUFFLE_MARKETPLACE_EVENT_TYPES,
    SOUFFLE_LISTINGS_TABLE_HANDLE,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    MarketplaceName,
    StandardMarketplaceEventType,
    ListingTableMetadata,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from processors.nft_orderbooks.models.nft_marketplace_listings_models import (
    NFTMarketplaceListing,
    CurrentNFTMarketplaceListing,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    parse_transaction_metadata,
    lookup_current_listing_in_db,
)
from utils import event_utils, general_utils, transaction_utils
from utils.session import Session
from utils.token_utils import TokenDataIdType, standardize_address


def parse_event(
    transaction: transaction_pb2.Transaction,
    event: transaction_pb2.Event,
    event_index: int,
) -> List[NFTMarketplaceEvent]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)

    # Readable transaction event type
    display_event_type = event_utils.get_event_type_short(event)
    display_event_type = display_event_type.replace("<0x1::aptos_coin::AptosCoin>", "")

    if display_event_type not in SOUFFLE_MARKETPLACE_EVENT_TYPES:
        return []

    data = json.loads(event.data)

    # Collection, token, and creator parsing
    token_data_id_struct = data.get("token_id", {}).get("token_data_id", {})
    collection = token_data_id_struct.get("collection", None) or ""
    token_name = token_data_id_struct.get("name", None) or ""
    creator = token_data_id_struct.get("creator", None) or ""

    token_data_id_type = TokenDataIdType(creator, collection, token_name)

    # Price parsing
    price = (
        data.get("price")
        or data.get("coin_per_token")
        or data.get("coin_amount")
        or None
    )

    # Token amount parsing
    token_amount = int(data.get("token_amount", 1))

    standard_event_type = standardize_marketplace_event_type(display_event_type)

    # Buyer and seller parsing
    buyer = data.get("buyer", None)
    if standard_event_type == StandardMarketplaceEventType.LISTING_CANCEL:
        seller = (
            transaction_utils.get_sender(user_transaction) if user_transaction else None
        )
    else:
        seller = data.get("token_owner", None)

    activity = NFTMarketplaceEvent(
        transaction_version=transaction_metadata.transaction_version,
        event_index=event_index,
        event_type=display_event_type,
        standard_event_type=standardize_marketplace_event_type(
            display_event_type
        ).value,
        creator_address=standardize_address(creator),
        collection=token_data_id_type.get_collection_trunc(),
        token_name=token_data_id_type.get_name_trunc(),
        token_data_id=token_data_id_type.to_hash(),
        collection_id=token_data_id_type.get_collection_data_id_hash(),
        price=price,
        token_amount=token_amount,
        buyer=standardize_address(buyer) if buyer else None,
        seller=standardize_address(seller) if seller else None,
        json_data=event.data,
        marketplace=MarketplaceName.SOUFFLE.value,
        contract_address=transaction_metadata.contract_address,
        entry_function_id_str=transaction_metadata.entry_function_id_str_short,
        transaction_timestamp=transaction_metadata.transaction_timestamp,
    )

    # Handle listing fill and cancel events
    if standard_event_type == StandardMarketplaceEventType.LISTING_FILLED:
        listing = NFTMarketplaceListing(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=token_data_id_type.get_creator(),
            token_name=token_data_id_type.get_name_trunc(),
            token_data_id=token_data_id_type.to_hash(),
            collection=token_data_id_type.get_collection_trunc(),
            collection_id=token_data_id_type.get_collection_data_id_hash(),
            price=price,
            token_amount=token_amount * -1,  # Negative if listing is filled
            seller=seller,
            buyer=buyer,
            event_type=standard_event_type.value,
            marketplace=MarketplaceName.SOUFFLE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_listing = CurrentNFTMarketplaceListing(
            creator_address=token_data_id_type.get_creator(),
            token_name=token_data_id_type.get_name_trunc(),
            token_data_id=token_data_id_type.to_hash(),
            collection=token_data_id_type.get_collection_trunc(),
            collection_id=token_data_id_type.get_collection_data_id_hash(),
            price=price,
            token_amount=0,  # Zero if listing is filled
            seller=seller,
            is_deleted=True,
            marketplace=MarketplaceName.SOUFFLE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(listing)
        parsed_objs.append(current_listing)
    elif standard_event_type == StandardMarketplaceEventType.LISTING_CANCEL:
        # Lookup previous listing price
        previous_price = 0
        old_listing_metadata = lookup_current_listing_in_db(
            token_data_id_type.to_hash()
        )
        if old_listing_metadata:
            previous_price = old_listing_metadata.price

        listing = NFTMarketplaceListing(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=token_data_id_type.get_creator(),
            token_name=token_data_id_type.get_name_trunc(),
            token_data_id=token_data_id_type.to_hash(),
            collection=token_data_id_type.get_collection_trunc(),
            collection_id=token_data_id_type.get_collection_data_id_hash(),
            price=previous_price,
            token_amount=token_amount * -1,  # Negative if listing is canceled
            seller=seller,
            event_type=standard_event_type.value,
            marketplace=MarketplaceName.SOUFFLE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_listing = CurrentNFTMarketplaceListing(
            creator_address=token_data_id_type.get_creator(),
            token_name=token_data_id_type.get_name_trunc(),
            token_data_id=token_data_id_type.to_hash(),
            collection=token_data_id_type.get_collection_trunc(),
            collection_id=token_data_id_type.get_collection_data_id_hash(),
            price=previous_price,
            token_amount=0,  # Zero if listing is canceled
            seller=seller,
            is_deleted=True,
            marketplace=MarketplaceName.SOUFFLE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(listing)
        parsed_objs.append(current_listing)

    parsed_objs.append(activity)

    return parsed_objs


def parse_write_table_item(
    transaction: transaction_pb2.Transaction,
    write_table_item: transaction_pb2.WriteTableItem,
    wsc_index: int,
) -> List[NFTMarketplaceEvent | NFTMarketplaceListing | CurrentNFTMarketplaceListing]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)
    table_handle = write_table_item.handle

    # Handle listing place event
    if table_handle == SOUFFLE_LISTINGS_TABLE_HANDLE:
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
            marketplace=MarketplaceName.SOUFFLE.value,
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
            marketplace=MarketplaceName.SOUFFLE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(listing)
        parsed_objs.append(current_listing)

    return parsed_objs


def parse_place_listing(
    write_table_item: transaction_pb2.WriteTableItem,
) -> ListingTableMetadata:
    table_key = json.loads(write_table_item.data.key)
    table_value = json.loads(write_table_item.data.value)

    # Collection, token, and creator parsing
    token_data_id_struct = table_key.get("token_data_id", {})
    collection = token_data_id_struct.get("collection", None)
    token_name = token_data_id_struct.get("name", None)
    creator = token_data_id_struct.get("creator", None)

    token_data_id_type = TokenDataIdType(creator, collection, token_name)

    # Price parsing
    price = table_value.get("coin_per_token", None)
    amount = 1 if price else 0

    # Seller parsing
    seller = table_value.get("token_owner", None)
    seller = standardize_address(seller) if seller else None

    return ListingTableMetadata(
        creator_address=token_data_id_type.get_creator(),
        collection=token_data_id_type.get_collection_trunc(),
        token_name=token_data_id_type.get_name_trunc(),
        token_data_id=token_data_id_type.to_hash(),
        collection_id=token_data_id_type.get_collection_data_id_hash(),
        price=price,
        amount=amount,
        seller=seller,
    )


def standardize_marketplace_event_type(
    marketplace_event_type: str,
) -> StandardMarketplaceEventType:
    match marketplace_event_type:
        case "FixedPriceMarket::BuyTokenEvent":
            return StandardMarketplaceEventType.LISTING_FILLED
        case "FixedPriceMarket::ListTokenEvent":
            return StandardMarketplaceEventType.LISTING_PLACE
        case "FixedPriceMarket::CancelListTokenEvent":
            return StandardMarketplaceEventType.LISTING_CANCEL
        case _:
            return StandardMarketplaceEventType.UNKNOWN
