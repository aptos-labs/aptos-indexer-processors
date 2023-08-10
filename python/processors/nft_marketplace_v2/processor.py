import json

from typing import Dict, List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName
from processors.nft_marketplace_v2.nft_marketplace_models import (
    CurrentNFTMarketplaceListing,
    CurrentNFTMarketplaceTokenOffer,
    CurrentNFTMarketplaceCollectionOffer,
    CurrentNFTMarketplaceAuction,
    NFTMarketplaceActivities,
)
from utils.object_utils import get_object_core, ObjectCore
from utils.token_utils import TokenStandard
from utils.transactions_processor import TransactionsProcessor, ProcessingResult
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
    get_token_offer_metadata,
    get_token_offer_v1,
    get_token_offer_v2,
    get_collection_offer_metadata,
    get_collection_offer_v1,
    get_collection_offer_v2,
    get_auction_listing,
    ListingMetadata,
    FixedPriceListing,
    ListingTokenV1Container,
    TokenMetadata,
    CollectionMetadata,
    TokenOfferMetadata,
    TokenOfferV1,
    TokenOfferV2,
    CollectionOfferMetadata,
    CollectionOfferV1,
    CollectionOfferV2,
    CollectionOfferEventMetadata,
    AuctionListing,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    StandardMarketplaceEventType,
    MarketplaceName,
)
from utils.processor_name import ProcessorName
from utils.token_utils import TokenStandard
from utils.models.schema_names import NFT_MARKETPLACE_V2_SCHEMA_NAME
from utils.session import Session
from sqlalchemy.dialects.postgresql import insert
from utils.config import Config


class NFTMarketplaceV2Processor(TransactionsProcessor):
    def name(self) -> str:
        return ProcessorName.NFT_MARKETPLACE_V2_PROCESSOR.value

    def schema(self) -> str:
        return NFT_MARKETPLACE_V2_SCHEMA_NAME

    def process_transactions(
        self,
        transactions: list[transaction_pb2.Transaction],
        start_version: int,
        end_version: int,
    ) -> ProcessingResult:
        assert self.config.custom_config, "Custom config must be set for this processor"
        marketplace_contract_address = str(
            self.config.custom_config["marketplace_contract_address"]
        )

        for transaction in transactions:
            user_transaction = transaction_utils.get_user_transaction(transaction)

            if not user_transaction:
                continue

            # Token metadatas parsed from events. The key is generated token_data_id for token v1,
            # and token address for token v2.
            token_metadatas: Dict[str, TokenMetadata] = {}
            # Collection metaddatas parsed from events. The key is generated collection_id for token v1,
            # and collection address for token v2.
            collection_metadatas: Dict[str, CollectionMetadata] = {}

            nft_marketplace_activities: List[NFTMarketplaceActivities] = []
            current_nft_marketplace_listings: List[CurrentNFTMarketplaceListing] = []
            current_token_offers: List[CurrentNFTMarketplaceTokenOffer] = []
            current_collection_offers: List[CurrentNFTMarketplaceCollectionOffer] = []
            current_auctions: List[CurrentNFTMarketplaceAuction] = []

            collection_offer_filled_metadatas: Dict[
                str, CollectionOfferEventMetadata
            ] = {}

            transaction_metadata = parse_transaction_metadata(transaction)
            events = user_transaction.events
            write_set_changes = transaction_utils.get_write_set_changes(transaction)

            # Get coin type
            type_args = (
                user_transaction.request.payload.entry_function_payload.type_arguments
            )
            coin_type = type_args[0] if type_args else None
            if coin_type:
                coin_type = transaction_utils.get_move_type_str(coin_type)

            # Loop 1
            # Parse all activities related to token listings, token and collection offers
            # The contract actually gives token/collection related metadata so we don't have to
            # look up anything
            for event_index, event in enumerate(events):
                qualified_event_type = str(event.type_str)

                if marketplace_contract_address not in qualified_event_type:
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
                    "events::AuctionBidEvent",
                ]:
                    continue

                data = json.loads(event.data)
                price = data.get("price")
                offer_or_listing_id = standardize_address(
                    data.get("listing")
                    or data.get("collection_offer")
                    or data.get("token_offer")
                )
                fee_schedule_id = event.key.account_address

                activity = None
                current_listing = None
                current_token_offer = None
                current_auction = None

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

                        listing_type = data.get("type")
                        assert listing_type

                        activity = NFTMarketplaceActivities(
                            transaction_version=transaction.version,
                            event_index=event_index,
                            offer_or_listing_id=offer_or_listing_id,
                            fee_schedule_id=fee_schedule_id,
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
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            event_type=StandardMarketplaceEventType.LISTING_FILLED.value,
                            transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )

                        if listing_type == "auction":
                            current_auction = CurrentNFTMarketplaceAuction(
                                listing_id=offer_or_listing_id,
                                token_data_id=token_metadata["token_data_id"],
                                collection_id=token_metadata["collection_id"],
                                fee_schedule_id=fee_schedule_id,
                                seller=standardize_address(data.get("seller")),
                                current_bid_price=price,
                                current_bidder=standardize_address(
                                    data.get("purchaser")
                                ),
                                starting_bid_price=0,
                                buy_it_now_price=None,
                                token_amount=1,
                                expiration_time=0,
                                is_deleted=True,
                                token_standard=TokenStandard.V2.value,
                                marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                                coin_type=coin_type,
                                contract_address=transaction_metadata.contract_address,
                                entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                                last_transaction_version=transaction.version,
                                last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                            )
                        else:
                            current_listing = CurrentNFTMarketplaceListing(
                                token_data_id=token_metadata["token_data_id"],
                                listing_id=offer_or_listing_id,
                                fee_schedule_id=fee_schedule_id,
                                collection_id=token_metadata["collection_id"],
                                price=price,
                                token_amount=0,
                                token_standard=token_metadata["token_standard"].value,
                                seller=standardize_address(data.get("seller")),
                                is_deleted=True,
                                coin_type=coin_type,
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
                            fee_schedule_id=fee_schedule_id,
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
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            event_type=StandardMarketplaceEventType.LISTING_CANCEL.value,
                            transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                        current_listing = CurrentNFTMarketplaceListing(
                            token_data_id=token_metadata["token_data_id"],
                            listing_id=offer_or_listing_id,
                            fee_schedule_id=fee_schedule_id,
                            collection_id=token_metadata["collection_id"],
                            price=price,
                            token_amount=0,
                            token_standard=token_metadata["token_standard"].value,
                            seller=standardize_address(data.get("seller")),
                            is_deleted=True,
                            coin_type=coin_type,
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
                            fee_schedule_id=fee_schedule_id,
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
                            coin_type=coin_type,
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
                            fee_schedule_id=fee_schedule_id,
                            collection_id=collection_metadata["collection_id"],
                            token_data_id=None,
                            creator_address=collection_metadata["creator_address"],
                            collection_name=collection_metadata["collection_name"],
                            token_name=None,
                            property_version=None,
                            price=price,
                            token_amount=data.get("token_amount"),
                            token_standard=collection_metadata["token_standard"].value,
                            seller=None,
                            buyer=standardize_address(data.get("purchaser")),
                            coin_type=coin_type,
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
                            fee_schedule_id=fee_schedule_id,
                            collection_id=collection_metadata["collection_id"],
                            token_data_id=None,
                            creator_address=collection_metadata["creator_address"],
                            collection_name=collection_metadata["collection_name"],
                            token_name=None,
                            property_version=None,
                            price=price,
                            token_amount=data.get("remaining_token_amount"),
                            token_standard=collection_metadata["token_standard"].value,
                            seller=None,
                            buyer=standardize_address(data.get("purchaser")),
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            event_type=StandardMarketplaceEventType.BID_CHANGE.value,
                            transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                        current_collection_offer = CurrentNFTMarketplaceCollectionOffer(
                            collection_offer_id=offer_or_listing_id,
                            collection_id=collection_metadata["collection_id"],
                            fee_schedule_id=fee_schedule_id,
                            buyer=standardize_address(data.get("purchaser")),
                            item_price=price,
                            remaining_token_amount=data.get("remaining_token_amount"),
                            expiration_time=0,
                            is_deleted=True,
                            token_standard=collection_metadata["token_standard"].value,
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            last_transaction_version=transaction.version,
                            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                        current_collection_offers.append(current_collection_offer)
                    case "events::CollectionOfferFilledEvent":
                        assert token_metadata
                        activity = NFTMarketplaceActivities(
                            transaction_version=transaction.version,
                            event_index=event_index,
                            offer_or_listing_id=offer_or_listing_id,
                            collection_id=token_metadata["collection_id"],
                            fee_schedule_id=fee_schedule_id,
                            token_data_id=token_metadata["token_data_id"],
                            creator_address=token_metadata["creator_address"],
                            collection_name=token_metadata["collection_name"],
                            token_name=token_metadata["token_name"],
                            property_version=token_metadata["property_version"],
                            price=price,
                            token_amount=data.get("remaining_token_amount", 0),
                            token_standard=token_metadata["token_standard"].value,
                            seller=standardize_address(data.get("seller")),
                            buyer=standardize_address(data.get("purchaser")),
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            event_type=StandardMarketplaceEventType.BID_CHANGE.value,
                            transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )

                        # The collection offer resource may be deleted after it is filled,
                        # so we need to parse collection offer metadata from the event
                        collection_offer_filled_metadata: CollectionOfferEventMetadata = {
                            "collection_offer_id": offer_or_listing_id,
                            "collection_metadata": {
                                "collection_id": token_metadata["collection_id"],
                                "creator_address": token_metadata["creator_address"],
                                "collection_name": token_metadata["collection_name"],
                                "token_standard": token_metadata["token_standard"],
                            },
                            "item_price": price,
                            "buyer": standardize_address(data.get("purchaser")),
                            "fee_schedule_id": fee_schedule_id,
                        }
                        collection_offer_filled_metadatas[
                            offer_or_listing_id
                        ] = collection_offer_filled_metadata
                    case "events::TokenOfferPlacedEvent":
                        assert token_metadata
                        activity = NFTMarketplaceActivities(
                            transaction_version=transaction.version,
                            event_index=event_index,
                            offer_or_listing_id=offer_or_listing_id,
                            fee_schedule_id=fee_schedule_id,
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
                            coin_type=coin_type,
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
                            fee_schedule_id=fee_schedule_id,
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
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            event_type=StandardMarketplaceEventType.BID_CHANGE.value,
                            transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                        current_token_offer = CurrentNFTMarketplaceTokenOffer(
                            offer_id=offer_or_listing_id,
                            token_data_id=token_metadata["token_data_id"],
                            collection_id=token_metadata["collection_id"],
                            fee_schedule_id=fee_schedule_id,
                            buyer=standardize_address(data.get("purchaser")),
                            price=price,
                            token_amount=0,
                            expiration_time=0,
                            is_deleted=True,
                            token_standard=token_metadata["token_standard"].value,
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            last_transaction_version=transaction.version,
                            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                    case "events::TokenOfferFilledEvent":
                        assert token_metadata
                        activity = NFTMarketplaceActivities(
                            transaction_version=transaction.version,
                            event_index=event_index,
                            offer_or_listing_id=offer_or_listing_id,
                            fee_schedule_id=fee_schedule_id,
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
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            event_type=StandardMarketplaceEventType.BID_FILLED.value,
                            transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                        current_token_offer = CurrentNFTMarketplaceTokenOffer(
                            offer_id=offer_or_listing_id,
                            token_data_id=token_metadata["token_data_id"],
                            collection_id=token_metadata["collection_id"],
                            fee_schedule_id=fee_schedule_id,
                            buyer=standardize_address(data.get("purchaser")),
                            price=price,
                            token_amount=0,
                            expiration_time=0,
                            is_deleted=True,
                            token_standard=token_metadata["token_standard"].value,
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            last_transaction_version=transaction.version,
                            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                    case "events::AuctionBidEvent":
                        assert token_metadata
                        activity = NFTMarketplaceActivities(
                            transaction_version=transaction.version,
                            event_index=event_index,
                            offer_or_listing_id=offer_or_listing_id,
                            fee_schedule_id=fee_schedule_id,
                            collection_id=token_metadata["collection_id"],
                            token_data_id=token_metadata["token_data_id"],
                            creator_address=token_metadata["creator_address"],
                            collection_name=token_metadata["collection_name"],
                            token_name=token_metadata["token_name"],
                            property_version=token_metadata["property_version"],
                            price=data.get("new_bid"),
                            token_amount=1,
                            token_standard=token_metadata["token_standard"].value,
                            seller=None,
                            buyer=standardize_address(data.get("new_bidder")),
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            event_type=StandardMarketplaceEventType.BID_PLACE.value,
                            transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                    case _:
                        continue

                nft_marketplace_activities.append(activity)
                if current_listing:
                    current_nft_marketplace_listings.append(current_listing)
                if current_token_offer:
                    current_token_offers.append(current_token_offer)
                if current_auction:
                    current_auctions.append(current_auction)

            # Object, listing and offer models. The key is the resource address
            object_metadatas: Dict[str, ObjectCore] = {}
            listing_metadatas: Dict[str, ListingMetadata] = {}
            fixed_price_listings: Dict[str, FixedPriceListing] = {}
            listing_token_v1_containers: Dict[str, ListingTokenV1Container] = {}
            token_offer_metadatas: Dict[str, TokenOfferMetadata] = {}
            token_offer_v1s: Dict[str, TokenOfferV1] = {}
            token_offer_v2s: Dict[str, TokenOfferV2] = {}
            collection_offer_metadatas: Dict[str, CollectionOfferMetadata] = {}
            collection_offer_v1s: Dict[str, CollectionOfferV1] = {}
            collection_offer_v2s: Dict[str, CollectionOfferV2] = {}
            auction_listings: Dict[str, AuctionListing] = {}

            # Loop 2
            # Parse out all the listing, auction, bid, and offer data from write set changes.
            # This is a bit more complicated than the other parsers because the data is spread out across multiple write set changes,
            # so we need a first loop to get all the data.
            for wsc_index, wsc in enumerate(write_set_changes):
                write_resource = write_set_change_utils.get_write_resource(wsc)
                if write_resource:
                    move_resource_address = standardize_address(write_resource.address)
                    move_resource_type = write_resource.type_str
                    move_resource_type_address = write_resource.type.address
                    data = json.loads(write_resource.data)

                    # Parse object metadata
                    object_core = get_object_core(move_resource_type, data)

                    if object_core:
                        object_metadatas[move_resource_address] = object_core

                    if move_resource_type_address != marketplace_contract_address:
                        continue

                    # Parse listing metadata
                    listing_metadata = get_listing_metadata(
                        move_resource_type, data, marketplace_contract_address
                    )
                    fixed_price_listing = get_fixed_priced_listing(
                        move_resource_type, data, marketplace_contract_address
                    )
                    listing_token_v1_container = get_listing_token_v1_container(
                        move_resource_type, data, marketplace_contract_address
                    )

                    if listing_metadata:
                        listing_metadatas[move_resource_address] = listing_metadata
                    if fixed_price_listing:
                        fixed_price_listings[
                            move_resource_address
                        ] = fixed_price_listing
                    if listing_token_v1_container:
                        listing_token_v1_containers[
                            move_resource_address
                        ] = listing_token_v1_container

                    # Parse token offer metadata
                    token_offer_metadata = get_token_offer_metadata(
                        move_resource_type, data, marketplace_contract_address
                    )
                    token_offer_v1 = get_token_offer_v1(
                        move_resource_type, data, marketplace_contract_address
                    )
                    token_offer_v2 = get_token_offer_v2(
                        move_resource_type, data, marketplace_contract_address
                    )

                    if token_offer_metadata:
                        token_offer_metadatas[
                            move_resource_address
                        ] = token_offer_metadata
                    if token_offer_v1:
                        token_offer_v1s[move_resource_address] = token_offer_v1
                    if token_offer_v2:
                        token_offer_v2s[move_resource_address] = token_offer_v2

                    # Parse collection offer metadata
                    collection_offer_metadata = get_collection_offer_metadata(
                        move_resource_type, data, marketplace_contract_address
                    )
                    collection_offer_v1 = get_collection_offer_v1(
                        move_resource_type, data, marketplace_contract_address
                    )
                    collection_offer_v2 = get_collection_offer_v2(
                        move_resource_type, data, marketplace_contract_address
                    )

                    if collection_offer_metadata:
                        collection_offer_metadatas[
                            move_resource_address
                        ] = collection_offer_metadata
                    if collection_offer_v1:
                        collection_offer_v1s[
                            move_resource_address
                        ] = collection_offer_v1
                    if collection_offer_v2:
                        collection_offer_v2s[
                            move_resource_address
                        ] = collection_offer_v2

                    # Parse auction metadata
                    auction_listing = get_auction_listing(
                        move_resource_type, data, marketplace_contract_address
                    )

                    if auction_listing:
                        auction_listings[move_resource_address] = auction_listing

            # Loop 3
            # Reconstruct the full listing and offer models and create DB objects
            for _, wsc in enumerate(write_set_changes):
                write_resource = write_set_change_utils.get_write_resource(wsc)
                if write_resource:
                    move_type_address = write_resource.type.address
                    if move_type_address != marketplace_contract_address:
                        continue

                    move_resource_address = standardize_address(write_resource.address)
                    move_resource_type = write_resource.type_str

                    if (
                        move_resource_type
                        == f"{marketplace_contract_address}::listing::Listing"
                    ):
                        # Get the data related to this listing that was parsed from loop 2
                        listing_metadata = listing_metadatas.get(move_resource_address)
                        fixed_price_listing = fixed_price_listings.get(
                            move_resource_address
                        )
                        auction_listing = auction_listings.get(move_resource_address)

                        assert (
                            listing_metadata
                        ), f"Listing metadata not found for txn {transaction.version}"

                        # If the listing is an auction, it will have an coin_listing::AuctionListing resource, otherwise
                        # it's a fixed price listing.
                        if auction_listing:
                            token_metadata = token_metadatas.get(
                                listing_metadata["token_address"]
                            )
                            assert (
                                token_metadata
                            ), f"Token metadata not found for txn {transaction.version}"

                            # Parses when auction is placed and when a bid is placed on an auction
                            current_auction = CurrentNFTMarketplaceAuction(
                                listing_id=move_resource_address,
                                token_data_id=listing_metadata["token_address"],
                                collection_id=token_metadata["collection_id"],
                                fee_schedule_id=listing_metadata["fee_schedule_id"],
                                seller=listing_metadata["seller"],
                                current_bid_price=auction_listing["current_bid_price"],
                                current_bidder=auction_listing["current_bidder"],
                                starting_bid_price=auction_listing[
                                    "starting_bid_price"
                                ],
                                buy_it_now_price=auction_listing["buy_it_now_price"],
                                token_amount=1,
                                expiration_time=auction_listing["auction_end_time"],
                                is_deleted=False,
                                token_standard=TokenStandard.V2.value,
                                coin_type=coin_type,
                                marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                                contract_address=transaction_metadata.contract_address,
                                entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                                last_transaction_version=transaction.version,
                                last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                            )
                            current_auctions.append(current_auction)
                        else:
                            assert (
                                fixed_price_listing
                            ), f"Fixed price listing not found for txn {transaction.version}"

                            token_address = listing_metadata["token_address"]
                            token_v1_container = listing_token_v1_containers.get(
                                token_address
                            )

                            current_listing = None

                            if token_v1_container:
                                token_v1_metadata = token_v1_container["token_metadata"]
                                current_listing = CurrentNFTMarketplaceListing(
                                    token_data_id=token_v1_metadata["token_data_id"],
                                    listing_id=move_resource_address,
                                    fee_schedule_id=listing_metadata["fee_schedule_id"],
                                    collection_id=token_v1_metadata["collection_id"],
                                    price=fixed_price_listing["price"],
                                    token_amount=token_v1_container["amount"],
                                    token_standard=TokenStandard.V1.value,
                                    seller=listing_metadata["seller"],
                                    is_deleted=False,
                                    coin_type=coin_type,
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
                                    listing_id=move_resource_address,
                                    fee_schedule_id=listing_metadata["fee_schedule_id"],
                                    collection_id=token_v2_metadata["collection_id"],
                                    price=fixed_price_listing["price"],
                                    token_amount=1,
                                    token_standard=TokenStandard.V2.value,
                                    seller=listing_metadata["seller"],
                                    is_deleted=False,
                                    coin_type=coin_type,
                                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                                    contract_address=transaction_metadata.contract_address,
                                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                                    last_transaction_version=transaction.version,
                                    last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                                )

                            current_nft_marketplace_listings.append(current_listing)

                    elif (
                        move_resource_type
                        == f"{marketplace_contract_address}::token_offer::TokenOffer"
                    ):
                        # Get the data related to this token offer that was parsed from loop 2
                        token_offer_object = object_metadatas.get(move_resource_address)
                        token_offer_metadata = token_offer_metadatas.get(
                            move_resource_address
                        )
                        token_offer_v1 = token_offer_v1s.get(move_resource_address)

                        assert (
                            token_offer_object
                        ), f"Token offer object not found for txn {transaction.version}"
                        assert (
                            token_offer_metadata
                        ), f"Token offer metadata not found for txn {transaction.version}"

                        current_token_offer = None

                        if token_offer_v1:
                            token_metadata = token_offer_v1["token_metadata"]
                            current_token_offer = CurrentNFTMarketplaceTokenOffer(
                                offer_id=move_resource_address,
                                token_data_id=token_metadata["token_data_id"],
                                collection_id=token_metadata["collection_id"],
                                fee_schedule_id=token_offer_metadata["fee_schedule_id"],
                                buyer=token_offer_object["owner"],
                                price=token_offer_metadata["price"],
                                token_amount=1,
                                expiration_time=token_offer_metadata["expiration_time"],
                                is_deleted=False,
                                token_standard=TokenStandard.V1.value,
                                coin_type=coin_type,
                                marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                                contract_address=transaction_metadata.contract_address,
                                entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                                last_transaction_version=transaction.version,
                                last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                            )
                        else:
                            token_offer_v2 = token_offer_v2s.get(move_resource_address)

                            assert (
                                token_offer_v2
                            ), f"Token offer v2 metadata not found for txn {transaction.version}"

                            token_v2_metadata = token_metadatas.get(
                                token_offer_v2["token_address"]
                            )
                            assert (
                                token_v2_metadata
                            ), f"Token v2 metadata not found for txn {transaction.version}"

                            current_token_offer = CurrentNFTMarketplaceTokenOffer(
                                offer_id=move_resource_address,
                                token_data_id=token_offer_v2["token_address"],
                                collection_id=token_v2_metadata["collection_id"],
                                fee_schedule_id=token_offer_metadata["fee_schedule_id"],
                                buyer=token_offer_object["owner"],
                                price=token_offer_metadata["price"],
                                token_amount=1,
                                expiration_time=token_offer_metadata["expiration_time"],
                                is_deleted=False,
                                token_standard=TokenStandard.V2.value,
                                coin_type=coin_type,
                                marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                                contract_address=transaction_metadata.contract_address,
                                entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                                last_transaction_version=transaction.version,
                                last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                            )

                        current_token_offers.append(current_token_offer)
                    elif (
                        move_resource_type
                        == f"{marketplace_contract_address}::collection_offer::CollectionOffer"
                    ):
                        # Get the data related to this collection offer that was parsed from loop 2
                        collection_offer_metadata = collection_offer_metadatas.get(
                            move_resource_address
                        )
                        collection_object = object_metadatas.get(move_resource_address)
                        collection_offer_v1 = collection_offer_v1s.get(
                            move_resource_address
                        )

                        assert (
                            collection_offer_metadata
                        ), f"Collection offer metadata not found for txn {transaction.version}"
                        assert (
                            collection_object
                        ), f"Collection object not found for txn {transaction.version}"

                        current_collection_offer = None

                        if collection_offer_v1:
                            current_collection_offer = CurrentNFTMarketplaceCollectionOffer(
                                collection_offer_id=move_resource_address,
                                collection_id=collection_offer_v1[
                                    "collection_metadata"
                                ]["collection_id"],
                                fee_schedule_id=collection_offer_metadata[
                                    "fee_schedule_id"
                                ],
                                buyer=collection_object["owner"],
                                item_price=collection_offer_metadata["item_price"],
                                remaining_token_amount=collection_offer_metadata[
                                    "remaining_token_amount"
                                ],
                                expiration_time=collection_offer_metadata[
                                    "expiration_time"
                                ],
                                is_deleted=False,
                                token_standard=TokenStandard.V1.value,
                                coin_type=coin_type,
                                marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                                contract_address=transaction_metadata.contract_address,
                                entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                                last_transaction_version=transaction.version,
                                last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                            )
                        else:
                            collection_offer_v2 = collection_offer_v2s.get(
                                move_resource_address
                            )
                            assert (
                                collection_offer_v2
                            ), f"Collection offer v2 not found for txn {transaction.version}"

                            current_collection_offer = CurrentNFTMarketplaceCollectionOffer(
                                collection_offer_id=move_resource_address,
                                collection_id=collection_offer_v2["collection_address"],
                                fee_schedule_id=collection_offer_metadata[
                                    "fee_schedule_id"
                                ],
                                buyer=collection_object["owner"],
                                item_price=collection_offer_metadata["item_price"],
                                remaining_token_amount=collection_offer_metadata[
                                    "remaining_token_amount"
                                ],
                                expiration_time=collection_offer_metadata[
                                    "expiration_time"
                                ],
                                is_deleted=False,
                                token_standard=TokenStandard.V2.value,
                                coin_type=coin_type,
                                marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                                contract_address=transaction_metadata.contract_address,
                                entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                                last_transaction_version=transaction.version,
                                last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                            )
                        current_collection_offers.append(current_collection_offer)

                delete_resource = write_set_change_utils.get_delete_resource(wsc)
                if delete_resource:
                    move_resource_address = standardize_address(delete_resource.address)

                    # If a collection offer resource gets deleted, that means it the offer was filled completely
                    # and we handle that here.
                    maybe_collection_offer_filled_metadata = (
                        collection_offer_filled_metadatas.get(move_resource_address)
                    )
                    if maybe_collection_offer_filled_metadata:
                        collection_metadata = maybe_collection_offer_filled_metadata[
                            "collection_metadata"
                        ]
                        current_collection_offer = CurrentNFTMarketplaceCollectionOffer(
                            collection_offer_id=move_resource_address,
                            collection_id=collection_metadata["collection_id"],
                            fee_schedule_id=maybe_collection_offer_filled_metadata[
                                "fee_schedule_id"
                            ],
                            buyer=maybe_collection_offer_filled_metadata["buyer"],
                            item_price=maybe_collection_offer_filled_metadata[
                                "item_price"
                            ],
                            remaining_token_amount=0,
                            expiration_time=0,
                            is_deleted=True,
                            token_standard=collection_metadata["token_standard"].value,
                            coin_type=coin_type,
                            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                            contract_address=transaction_metadata.contract_address,
                            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                            last_transaction_version=transaction.version,
                            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                        )
                        current_collection_offers.append(current_collection_offer)

            self.insert_nft_activities(nft_marketplace_activities)
            self.insert_nft_listings(current_nft_marketplace_listings)
            self.insert_nft_token_offers(current_token_offers)
            self.insert_nft_collection_offers(current_collection_offers)
            self.insert_nft_auctions(current_auctions)

        return ProcessingResult(
            start_version=start_version,
            end_version=end_version,
        )

    def insert_nft_activities(
        self,
        activities: List[NFTMarketplaceActivities],
    ) -> None:
        if not activities:
            return

        # Sort by pk to avoid postgres deadlock since we're doing multi threaded db writes
        activities = sorted(
            activities, key=lambda x: (x.transaction_version, x.event_index)
        )
        activites_dict = [
            {
                "transaction_version": activity.transaction_version,
                "event_index": activity.event_index,
                "offer_or_listing_id": activity.offer_or_listing_id,
                "fee_schedule_id": activity.fee_schedule_id,
                "collection_id": activity.collection_id,
                "token_data_id": activity.token_data_id,
                "creator_address": activity.creator_address,
                "collection_name": activity.collection_name,
                "token_name": activity.token_name,
                "property_version": activity.property_version,
                "price": activity.price,
                "token_amount": activity.token_amount,
                "token_standard": activity.token_standard,
                "seller": activity.seller,
                "buyer": activity.buyer,
                "coin_type": activity.coin_type,
                "marketplace": activity.marketplace,
                "contract_address": activity.contract_address,
                "entry_function_id_str": activity.entry_function_id_str,
                "event_type": activity.event_type,
                "transaction_timestamp": activity.transaction_timestamp,
            }
            for activity in activities
        ]
        with Session() as session, session.begin():
            insert_stmt = insert(NFTMarketplaceActivities).values(activites_dict)
            do_update_stmt = insert_stmt.on_conflict_do_update(
                constraint="nft_marketplace_activities_pkey",
                set_=dict(insert_stmt.excluded.items()),
            )
            session.execute(do_update_stmt)

    def insert_nft_listings(
        self,
        listings: List[CurrentNFTMarketplaceListing],
    ) -> None:
        if not listings:
            return

        # Sort by pk to avoid postgres deadlock since we're doing multi threaded db writes
        listings = sorted(listings, key=lambda x: (x.listing_id, x.token_data_id))
        listings_dict = [
            {
                "listing_id": listing.listing_id,
                "token_data_id": listing.token_data_id,
                "collection_id": listing.collection_id,
                "fee_schedule_id": listing.fee_schedule_id,
                "seller": listing.seller,
                "price": listing.price,
                "token_amount": listing.token_amount,
                "is_deleted": listing.is_deleted,
                "token_standard": listing.token_standard,
                "coin_type": listing.coin_type,
                "marketplace": listing.marketplace,
                "contract_address": listing.contract_address,
                "entry_function_id_str": listing.entry_function_id_str,
                "last_transaction_version": listing.last_transaction_version,
                "last_transaction_timestamp": listing.last_transaction_timestamp,
            }
            for listing in listings
        ]
        with Session() as session, session.begin():
            insert_stmt = insert(CurrentNFTMarketplaceListing).values(listings_dict)
            # Only update if the new transaction version is greater than the current one
            do_update_stmt = insert_stmt.on_conflict_do_update(
                constraint="current_nft_marketplace_listings_pkey",
                set_=dict(insert_stmt.excluded.items()),
                where=(
                    insert_stmt.excluded["last_transaction_version"]
                    > CurrentNFTMarketplaceListing.last_transaction_version
                ),
            )
            session.execute(do_update_stmt)

    def insert_nft_token_offers(
        self,
        offers: List[CurrentNFTMarketplaceTokenOffer],
    ) -> None:
        if not offers:
            return

        # Sort by pk to avoid postgres deadlock since we're doing multi threaded db writes
        offers = sorted(offers, key=lambda x: (x.offer_id, x.token_data_id))
        offers_dict = [
            {
                "offer_id": offer.offer_id,
                "token_data_id": offer.token_data_id,
                "collection_id": offer.collection_id,
                "fee_schedule_id": offer.fee_schedule_id,
                "buyer": offer.buyer,
                "price": offer.price,
                "token_amount": offer.token_amount,
                "expiration_time": offer.expiration_time,
                "is_deleted": offer.is_deleted,
                "token_standard": offer.token_standard,
                "coin_type": offer.coin_type,
                "marketplace": offer.marketplace,
                "contract_address": offer.contract_address,
                "entry_function_id_str": offer.entry_function_id_str,
                "last_transaction_version": offer.last_transaction_version,
                "last_transaction_timestamp": offer.last_transaction_timestamp,
            }
            for offer in offers
        ]
        with Session() as session, session.begin():
            insert_stmt = insert(CurrentNFTMarketplaceTokenOffer).values(offers_dict)
            # Only update if the new transaction version is greater than the current one
            do_update_stmt = insert_stmt.on_conflict_do_update(
                constraint="current_nft_marketplace_token_offers_pkey",
                set_=dict(insert_stmt.excluded.items()),
                where=(
                    insert_stmt.excluded["last_transaction_version"]
                    > CurrentNFTMarketplaceTokenOffer.last_transaction_version
                ),
            )
            session.execute(do_update_stmt)

    def insert_nft_collection_offers(
        self,
        offers: List[CurrentNFTMarketplaceCollectionOffer],
    ) -> None:
        if not offers:
            return

        # Sort by pk to avoid postgres deadlock since we're doing multi threaded db writes
        offers = sorted(offers, key=lambda x: (x.collection_offer_id, x.collection_id))
        offers_dict = [
            {
                "collection_offer_id": offer.collection_offer_id,
                "collection_id": offer.collection_id,
                "fee_schedule_id": offer.fee_schedule_id,
                "buyer": offer.buyer,
                "item_price": offer.item_price,
                "remaining_token_amount": offer.remaining_token_amount,
                "expiration_time": offer.expiration_time,
                "is_deleted": offer.is_deleted,
                "token_standard": offer.token_standard,
                "coin_type": offer.coin_type,
                "marketplace": offer.marketplace,
                "contract_address": offer.contract_address,
                "entry_function_id_str": offer.entry_function_id_str,
                "last_transaction_version": offer.last_transaction_version,
                "last_transaction_timestamp": offer.last_transaction_timestamp,
            }
            for offer in offers
        ]
        with Session() as session, session.begin():
            insert_stmt = insert(CurrentNFTMarketplaceCollectionOffer).values(
                offers_dict
            )
            # Only update if the new transaction version is greater than the current one
            do_update_stmt = insert_stmt.on_conflict_do_update(
                constraint="current_nft_marketplace_collection_offers_pkey",
                set_=dict(insert_stmt.excluded.items()),
                where=(
                    insert_stmt.excluded["last_transaction_version"]
                    > CurrentNFTMarketplaceCollectionOffer.last_transaction_version
                ),
            )
            session.execute(do_update_stmt)

    def insert_nft_auctions(
        self,
        auctions: List[CurrentNFTMarketplaceAuction],
    ) -> None:
        if not auctions:
            return

        # Sort by pk to avoid postgres deadlock since we're doing multi threaded db writes
        auctions = sorted(auctions, key=lambda x: (x.listing_id, x.token_data_id))
        auctions_dict = [
            {
                "listing_id": auction.listing_id,
                "token_data_id": auction.token_data_id,
                "collection_id": auction.collection_id,
                "fee_schedule_id": auction.fee_schedule_id,
                "seller": auction.seller,
                "current_bid_price": auction.current_bid_price,
                "current_bidder": auction.current_bidder,
                "starting_bid_price": auction.starting_bid_price,
                "buy_it_now_price": auction.buy_it_now_price,
                "token_amount": auction.token_amount,
                "expiration_time": auction.expiration_time,
                "is_deleted": auction.is_deleted,
                "token_standard": auction.token_standard,
                "coin_type": auction.coin_type,
                "marketplace": auction.marketplace,
                "contract_address": auction.contract_address,
                "entry_function_id_str": auction.entry_function_id_str,
                "last_transaction_version": auction.last_transaction_version,
                "last_transaction_timestamp": auction.last_transaction_timestamp,
            }
            for auction in auctions
        ]
        with Session() as session, session.begin():
            insert_stmt = insert(CurrentNFTMarketplaceAuction).values(auctions_dict)
            # Only update if the new transaction version is greater than the current one
            do_update_stmt = insert_stmt.on_conflict_do_update(
                constraint="current_nft_marketplace_auctions_pkey",
                set_=dict(insert_stmt.excluded.items()),
                where=(
                    insert_stmt.excluded["last_transaction_version"]
                    > CurrentNFTMarketplaceAuction.last_transaction_version
                ),
            )
            session.execute(do_update_stmt)
