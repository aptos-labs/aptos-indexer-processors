import json
from typing import List, Optional
from typing_extensions import TypedDict
from aptos.transaction.v1 import transaction_pb2
from processors.nft_marketplace_v2.marketplace_v2_utils import (
    lookup_v2_current_listing_in_db,
)
from processors.nft_marketplace_v2.constants import MARKETPLACE_V2_ADDRESS
from processors.nft_marketplace_v2.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceActivities,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    parse_transaction_metadata,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    StandardMarketplaceEventType,
    MarketplaceName,
)
from utils import event_utils, transaction_utils, write_set_change_utils
from utils.token_utils import TokenStandard, TokenDataIdType, CollectionDataIdType
from utils.general_utils import standardize_address

TokenMetadata = TypedDict(
    "TokenMetadata",
    {
        "collection_id": str,
        "token_data_id": str,
        "creator_address": str,
        "collection_name": str,
        "token_name": str,
        "property_version": Optional[int],
        "token_standard": TokenStandard,
    },
)

CollectionMetadata = TypedDict(
    "CollectionMetadata",
    {
        "collection_id": str,
        "creator_address": str,
        "collection_name": str,
        "token_standard": TokenStandard,
    },
)


# Parse all activities related to token listings, token and collection offers
# The contract actually gives token/collection related metadata so we don't have to
# look up anything
def parse_activities(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceActivities]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)
    events = user_transaction.events

    for event_index, event in enumerate(events):
        qualified_event_type = event.type_str

        if MARKETPLACE_V2_ADDRESS not in qualified_event_type:
            continue

        event_type_short = event_utils.get_event_type_short(event)
        if event_type_short not in [
            "events::ListingFilledEvent",
            "events::ListingCanceledEvent",
            "events::ListingPlacedEvent",
            "events::CollectionOfferPlacedEvent",
            "events::CollectionOfferCanceledEvent",
            "events::CollectionOfferFilledEvent",
            "events::TokenOfferPlacedEvent",
            "events::TokenOfferCanceledEvent",
            "events::TokenOfferFilledEvent",
        ]:
            continue
        data = json.loads(event.data)
        price = data.get("price")
        offer_or_listing_id = standardize_address(
            data.get("listing")
            or data.get("collection_offer")
            or data.get("token_offer")
        )
        activity = None
        token_metadata = get_token_metadata(data)
        collection_metadata = get_collection_metadata(data)

        match event_type_short:
            case "events::ListingFilledEvent":
                assert token_metadata

                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=token_metadata["collection_id"],
                    token_data_id=token_metadata["token_data_id"],
                    creator_address=token_metadata["creator_address"],
                    collection_name=token_metadata["collection_name"],
                    token_name=token_metadata["token_name"],
                    property_version=token_metadata["property_version"],
                    price=price,
                    token_amount=1,
                    token_standard=token_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    buyer=standardize_address(data.get("purchaser")),
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.LISTING_FILLED.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::ListingCanceledEvent":
                assert token_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=token_metadata["collection_id"],
                    token_data_id=token_metadata["token_data_id"],
                    creator_address=token_metadata["creator_address"],
                    collection_name=token_metadata["collection_name"],
                    token_name=token_metadata["token_name"],
                    property_version=token_metadata["property_version"],
                    price=price,
                    token_amount=1,
                    token_standard=token_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    buyer=None,
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.LISTING_CANCEL.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::ListingPlacedEvent":
                assert token_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=token_metadata["collection_id"],
                    token_data_id=token_metadata["token_data_id"],
                    creator_address=token_metadata["creator_address"],
                    collection_name=token_metadata["collection_name"],
                    token_name=token_metadata["token_name"],
                    property_version=token_metadata["property_version"],
                    price=price,
                    token_amount=1,
                    token_standard=token_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    buyer=None,
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.LISTING_PLACE.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::CollectionOfferPlacedEvent":
                assert collection_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=collection_metadata["collection_id"],
                    token_data_id=None,
                    creator_address=collection_metadata["creator_address"],
                    collection_name=collection_metadata["collection_name"],
                    token_name=None,
                    property_version=None,
                    price=price,
                    token_amount=data.get("token_amount"),
                    token_standard=collection_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    buyer=standardize_address(data.get("purchaser")),
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.BID_PLACE.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::CollectionOfferCanceledEvent":
                assert collection_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=collection_metadata["collection_id"],
                    token_data_id=None,
                    creator_address=collection_metadata["creator_address"],
                    collection_name=collection_metadata["collection_name"],
                    token_name=None,
                    property_version=None,
                    price=price,
                    token_amount=data.get("remaining_token_amount"),
                    token_standard=collection_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    buyer=standardize_address(data.get("purchaser")),
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.BID_CHANGE.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::CollectionOfferFilledEvent":
                assert collection_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=collection_metadata["collection_id"],
                    token_data_id=None,
                    creator_address=collection_metadata["creator_address"],
                    collection_name=collection_metadata["collection_name"],
                    token_name=None,
                    property_version=None,
                    price=price,
                    token_amount=data.get("remaining_token_amount"),
                    token_standard=collection_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    buyer=standardize_address(data.get("purchaser")),
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.BID_CHANGE.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::TokenOfferPlacedEvent":
                assert token_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=token_metadata["collection_id"],
                    token_data_id=token_metadata["token_data_id"],
                    creator_address=token_metadata["creator_address"],
                    collection_name=token_metadata["collection_name"],
                    token_name=token_metadata["token_name"],
                    property_version=token_metadata["property_version"],
                    price=price,
                    token_amount=1,
                    token_standard=token_metadata["token_standard"].value,
                    seller=None,
                    buyer=standardize_address(data.get("purchaser")),
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.BID_PLACE.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::TokenOfferCanceledEvent":
                assert token_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=token_metadata["collection_id"],
                    token_data_id=token_metadata["token_data_id"],
                    creator_address=token_metadata["creator_address"],
                    collection_name=token_metadata["collection_name"],
                    token_name=token_metadata["token_name"],
                    property_version=token_metadata["property_version"],
                    price=price,
                    token_amount=0,
                    token_standard=token_metadata["token_standard"].value,
                    seller=None,
                    buyer=standardize_address(data.get("purchaser")),
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.BID_CHANGE.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case "events::TokenOfferFilledEvent":
                assert token_metadata
                activity = NFTMarketplaceActivities(
                    transaction_version=transaction.version,
                    event_index=event_index,
                    offer_or_listing_id=offer_or_listing_id,
                    collection_id=token_metadata["collection_id"],
                    token_data_id=token_metadata["token_data_id"],
                    creator_address=token_metadata["creator_address"],
                    collection_name=token_metadata["collection_name"],
                    token_name=token_metadata["token_name"],
                    property_version=token_metadata["property_version"],
                    price=price,
                    token_amount=1,
                    token_standard=token_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    buyer=standardize_address(data.get("purchaser")),
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=StandardMarketplaceEventType.BID_FILLED.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
            case _:
                continue
        parsed_objs.append(activity)

    return parsed_objs


# This only tracks listing, we still need to track offers somehow
def parse_listing(
    transaction: transaction_pb2.Transaction,
) -> List[CurrentNFTMarketplaceListing]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)
    events = user_transaction.events
    write_set_changes = transaction_utils.get_write_set_changes(transaction)

    # Parse listing deletes and fills
    for _, event in enumerate(events):
        qualified_event_type = event.type_str

        if MARKETPLACE_V2_ADDRESS not in qualified_event_type:
            continue

        event_type_short = event_utils.get_event_type_short(event)
        if event_type_short in [
            "events::ListingFilledEvent",
            "events::ListingCanceledEvent",
        ]:
            data = json.loads(event.data)
            price = data.get("price", None)

            if not price:
                continue

            listing_address = data.get("listing")
            if not listing_address:
                raise Exception("Listing address not found", data)

            listing_address = standardize_address(listing_address)
            current_listing_from_db = lookup_v2_current_listing_in_db(listing_address)

            if current_listing_from_db:
                seller = None
                price = int(price)

                current_listing = CurrentNFTMarketplaceListing(
                    # TODO: get this from the event
                    token_data_id=current_listing_from_db.token_data_id,
                    listing_address=listing_address,
                    price=price,
                    token_amount=0,
                    # TODO: instead of looking up just check for TokenV1Container or from the event
                    token_standard=current_listing_from_db.token_standard,
                    seller=current_listing_from_db.seller,
                    is_deleted=True,
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    last_transaction_version=transaction.version,
                    last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
                parsed_objs.extend([current_listing])

    listing_address = None
    inner = None
    token_data_id = None  # Token address for v2, token data id for v1
    price = None
    seller = None
    token_standard = TokenStandard.V2

    # Parse listing place
    for _, wsc in enumerate(write_set_changes):
        write_resource = write_set_change_utils.get_write_resource(wsc)
        if write_resource:
            move_type_address = write_resource.type.address
            if move_type_address != MARKETPLACE_V2_ADDRESS:
                continue

            move_type_short = write_set_change_utils.get_move_type_short(
                write_resource.type
            )
            data = json.loads(write_resource.data)

            match move_type_short:
                case "coin_listing::FixedPriceListing":
                    price = data.get("price", None)
                case "listing::Listing":
                    listing_address = standardize_address(write_resource.address)
                    seller = data.get("seller", None)
                    seller = standardize_address(seller) if seller else None
                    inner = data.get("object", {}).get("inner", None)

    # Parse token v1 container or v2 token
    for _, wsc in enumerate(write_set_changes):
        write_resource = write_set_change_utils.get_write_resource(wsc)
        if write_resource:
            resource_address = write_resource.address
            if resource_address != inner:
                continue
            data = json.loads(write_resource.data)
            move_type = write_resource.type_str

            if move_type == f"{MARKETPLACE_V2_ADDRESS}::listing::TokenV1Container":
                token_standard = TokenStandard.V1

                token_data_id_struct = (
                    data.get("token", {}).get("id", {}).get("token_data_id", {})
                )
                collection = token_data_id_struct.get("collection", None)
                creator = token_data_id_struct.get("creator", None)
                name = token_data_id_struct.get("name", None)

                token_data_id_type = TokenDataIdType(creator, collection, name)
                token_data_id = token_data_id_type.to_hash()
            elif move_type == "0x4::token::Token":
                token_data_id = resource_address

    if listing_address and token_data_id:
        current_listing = CurrentNFTMarketplaceListing(
            token_data_id=token_data_id,
            listing_address=listing_address,
            price=price,
            token_amount=1,
            token_standard=token_standard.value,
            seller=seller,
            is_deleted=False,
            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction.version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        parsed_objs.extend([current_listing])

    return parsed_objs


def get_token_metadata(data: dict) -> Optional[TokenMetadata]:
    token_metadata = data.get("token_metadata", {})
    if not token_metadata:
        return None
    token_v2 = token_metadata.get("token", {}).get("vec", [])
    creator_address = standardize_address(token_metadata["creator_address"])
    maybe_property_version = token_metadata.get("property_version", {}).get("vec", [])
    property_version = maybe_property_version[0] if maybe_property_version else None
    if token_v2:
        return {
            "collection_id": standardize_address(
                token_metadata.get("collection", {}).get("vec", [])[0]["inner"]
            ),
            "token_data_id": standardize_address(token_v2[0]["inner"]),
            "creator_address": creator_address,
            "collection_name": token_metadata["collection_name"],
            "token_name": token_metadata["token_name"],
            "property_version": property_version,
            "token_standard": TokenStandard.V2,
        }
    token_data_id_type = TokenDataIdType(
        creator_address,
        token_metadata["collection_name"],
        token_metadata["token_name"],
    )
    return {
        "collection_id": token_data_id_type.get_collection_data_id_hash(),
        "token_data_id": token_data_id_type.to_hash(),
        "creator_address": creator_address,
        "collection_name": token_metadata["collection_name"],
        "token_name": token_metadata["token_name"],
        "property_version": property_version,
        "token_standard": TokenStandard.V1,
    }


def get_collection_metadata(data: dict) -> Optional[CollectionMetadata]:
    collection_metadata = data.get("collection_metadata", {})
    if not collection_metadata:
        return None
    creator_address = standardize_address(collection_metadata["creator_address"])
    collection_v2 = collection_metadata.get("collection", {}).get("vec", [])
    if collection_v2:
        return {
            "collection_id": standardize_address(collection_v2[0].get("inner")),
            "creator_address": creator_address,
            "collection_name": collection_metadata["collection_name"],
            "token_standard": TokenStandard.V2,
        }
    collection = CollectionDataIdType(
        creator_address,
        collection_metadata["collection_name"],
    )
    return {
        "collection_id": collection.to_hash(),
        "creator_address": creator_address,
        "collection_name": collection_metadata["collection_name"],
        "token_standard": TokenStandard.V1,
    }
