import json

from typing import Dict, List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName
from processors.nft_orderbooks.nft_marketplace_constants import (
    MARKETPLACE_ENTRY_FUNCTIONS,
    MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV,
    MARKETPLACE_TABLE_HANDLES_INV,
)
from processors.nft_marketplace_v2 import marketplace_v2_parser
from processors.nft_marketplace_v2.constants import MARKETPLACE_V2_ADDRESS
from processors.nft_marketplace_v2.nft_marketplace_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceActivities,
)
from utils.token_utils import TokenStandard
from utils.transactions_processor import TransactionsProcessor
from utils import event_utils, transaction_utils, write_set_change_utils
from utils.general_utils import standardize_address
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    parse_transaction_metadata,
)
from processors.nft_marketplace_v2.marketplace_v2_parser import (
    get_token_metadata_from_event,
    get_collection_metadata_from_event,
    get_listing_metadata,
    get_fixed_priced_listing,
    get_listing_token_v1_container,
    ListingMetadata,
    FixedPriceListing,
    ListingTokenV1Container,
    TokenMetadata,
    CollectionMetadata,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    StandardMarketplaceEventType,
    MarketplaceName,
)
from utils.token_utils import TokenStandard, TokenDataIdType, CollectionDataIdType


def parse(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceActivities | CurrentNFTMarketplaceListing]:
    user_transaction = transaction_utils.get_user_transaction(transaction)

    if not user_transaction:
        return []

    # TODO: Optimize the loops

    # Token metadatas parsed from events. The key is generated token_data_id for token v1,
    # and token address for token v2.
    token_metadatas: Dict[str, TokenMetadata] = {}
    # Collection metaddatas parsed from events. The key is generated collection_id for token v1,
    # and collection address for token v2.
    collection_metadatas: Dict[str, CollectionMetadata] = {}

    nft_marketplace_activities: List[NFTMarketplaceActivities] = []
    current_nft_marketplace_listings: List[CurrentNFTMarketplaceListing] = []

    transaction_metadata = parse_transaction_metadata(transaction)
    events = user_transaction.events
    write_set_changes = transaction_utils.get_write_set_changes(transaction)

    # Parse all activities related to token listings, token and collection offers
    # The contract actually gives token/collection related metadata so we don't have to
    # look up anything
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
        current_listing = None

        token_metadata = get_token_metadata_from_event(data)
        collection_metadata = get_collection_metadata_from_event(data)

        if token_metadata:
            token_metadatas[token_metadata["token_data_id"]] = token_metadata

        if collection_metadata:
            collection_metadatas[
                collection_metadata["collection_id"]
            ] = collection_metadata

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

                current_listing = CurrentNFTMarketplaceListing(
                    token_data_id=token_metadata["token_data_id"],
                    listing_address=offer_or_listing_id,
                    price=price,
                    token_amount=0,
                    token_standard=token_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    is_deleted=True,
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    last_transaction_version=transaction.version,
                    last_transaction_timestamp=transaction_metadata.transaction_timestamp,
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
                current_listing = CurrentNFTMarketplaceListing(
                    token_data_id=token_metadata["token_data_id"],
                    listing_address=offer_or_listing_id,
                    price=price,
                    token_amount=0,
                    token_standard=token_metadata["token_standard"].value,
                    seller=standardize_address(data.get("seller")),
                    is_deleted=True,
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    last_transaction_version=transaction.version,
                    last_transaction_timestamp=transaction_metadata.transaction_timestamp,
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

        nft_marketplace_activities.append(activity)
        if current_listing:
            current_nft_marketplace_listings.append(current_listing)

    # Listing models. The key is the resource address
    listing_metadatas: Dict[str, ListingMetadata] = {}
    fixed_price_listings: Dict[str, FixedPriceListing] = {}
    listing_token_v1_containers: Dict[str, ListingTokenV1Container] = {}

    # Parse out all the listing, auction, bid, and offer data from write set changes.
    # This is a bit more complicated than the other parsers because the data is spread out across multiple write set changes,
    # so we need a first loop to get all the data.
    for _, wsc in enumerate(write_set_changes):
        write_resource = write_set_change_utils.get_write_resource(wsc)
        if write_resource:
            move_type_address = write_resource.type.address
            if move_type_address != MARKETPLACE_V2_ADDRESS:
                continue

            data = json.loads(write_resource.data)
            move_resource_address = standardize_address(write_resource.address)
            move_resource_type = write_resource.type_str

            listing_metadata = get_listing_metadata(move_resource_type, data)
            fixed_price_listing = get_fixed_priced_listing(move_resource_type, data)
            listing_token_v1_container = get_listing_token_v1_container(
                move_resource_type, data
            )

            if listing_metadata:
                listing_metadatas[move_resource_address] = listing_metadata
            if fixed_price_listing:
                fixed_price_listings[move_resource_address] = fixed_price_listing
            if listing_token_v1_container:
                listing_token_v1_containers[
                    move_resource_address
                ] = listing_token_v1_container

    # Reconstruct the full listing model and create DB objects
    for _, wsc in enumerate(write_set_changes):
        write_resource = write_set_change_utils.get_write_resource(wsc)
        if write_resource:
            move_type_address = write_resource.type.address
            if move_type_address != MARKETPLACE_V2_ADDRESS:
                continue

            move_resource_address = standardize_address(write_resource.address)
            move_resource_type = write_resource.type_str

            if move_resource_type == f"{MARKETPLACE_V2_ADDRESS}::listing::Listing":
                # Get the data related to this listing that was parsed from the previous loop
                listing_metadata = listing_metadatas.get(move_resource_address)
                fixed_price_listing = fixed_price_listings.get(move_resource_address)

                assert (
                    listing_metadata
                ), f"Listing metadata not found for txn {transaction.version}"
                assert (
                    fixed_price_listing
                ), f"Fixed price listing not found for txn {transaction.version}"



                token_address = listing_metadata["token_address"]
                token_v1_container = listing_token_v1_containers.get(token_address)

                current_listing = None

                if token_v1_container:
                    token_v1_metadata = token_v1_container["token_metadata"]
                    current_listing = CurrentNFTMarketplaceListing(
                        token_data_id=token_v1_metadata["token_data_id"],
                        listing_address=move_resource_address,
                        price=fixed_price_listing["price"],
                        token_amount=token_v1_container["amount"],
                        token_standard=TokenStandard.V1.value,
                        seller=listing_metadata["seller"],
                        is_deleted=False,
                        marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                        contract_address=transaction_metadata.contract_address,
                        entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                        last_transaction_version=transaction.version,
                        last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                    )
                else:
                    token_v2_metadata = token_metadatas.get(token_address)

                    assert (
                        token_v2_metadata
                    ), f"Token v2 metadata not found for txn {transaction.version}"

                    current_listing = CurrentNFTMarketplaceListing(
                        token_data_id=token_v2_metadata["token_data_id"],
                        listing_address=move_resource_address,
                        price=fixed_price_listing["price"],
                        token_amount=1,
                        token_standard=TokenStandard.V2.value,
                        seller=listing_metadata["seller"],
                        is_deleted=False,
                        marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                        contract_address=transaction_metadata.contract_address,
                        entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                        last_transaction_version=transaction.version,
                        last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                    )

                current_nft_marketplace_listings.append(current_listing)

    return nft_marketplace_activities + current_nft_marketplace_listings


if __name__ == "__main__":
    transactions_processor = TransactionsProcessor(
        parser_function=parse,
        processor_name="nft-marketplace-v2",
    )
    transactions_processor.process()
