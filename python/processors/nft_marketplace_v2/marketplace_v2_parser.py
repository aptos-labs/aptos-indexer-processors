import json
from dataclasses import dataclass
from typing import List, Optional, Tuple
from typing_extensions import TypedDict
from aptos.transaction.v1 import transaction_pb2
from processors.nft_marketplace_v2.marketplace_v2_utils import (
    lookup_v2_current_listing_in_db,
)
from processors.nft_marketplace_v2.constants import MARKETPLACE_V2_ADDRESS
from processors.nft_marketplace_v2.nft_marketplace_models import (
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


class TokenMetadata(TypedDict):
    collection_id: str
    token_data_id: str
    creator_address: str
    collection_name: str
    token_name: str
    property_version: Optional[int]
    token_standard: TokenStandard


class CollectionMetadata(TypedDict):
    collection_id: str
    creator_address: str
    collection_name: str
    token_standard: TokenStandard


def get_token_metadata_from_event(data: dict) -> Optional[TokenMetadata]:
    token_metadata = data.get("token_metadata", {})
    if not token_metadata:
        return None
    token_v2 = token_metadata.get("token", {}).get("vec", [])
    creator_address = standardize_address(token_metadata["creator_address"])
    maybe_property_version = token_metadata.get("property_version", {}).get("vec", [])
    property_version = maybe_property_version[0] if maybe_property_version else None

    # For token v2, token struct will contain token address
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

    # Token v1 parsing
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


def get_collection_metadata_from_event(data: dict) -> Optional[CollectionMetadata]:
    collection_metadata = data.get("collection_metadata", {})
    if not collection_metadata:
        return None
    creator_address = standardize_address(collection_metadata["creator_address"])
    collection_v2 = collection_metadata.get("collection", {}).get("vec", [])

    # For token v2, collection struct will contain collection address
    if collection_v2:
        return {
            "collection_id": standardize_address(collection_v2[0].get("inner")),
            "creator_address": creator_address,
            "collection_name": collection_metadata["collection_name"],
            "token_standard": TokenStandard.V2,
        }

    # Token v1 parsing
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


def get_token_metadata_from_wsc(
    move_resource_type: str, data: dict
) -> Optional[TokenMetadata]:
    if move_resource_type != "0x4::token::Token":
        return None

    collection = data.get("collection", {}).get("inner")


# Listing models
class FixedPriceListing(TypedDict):
    price: int


class ListingMetadata(TypedDict):
    seller: str
    # Either the token v2 address or the token v1 container address
    token_address: str


class ListingTokenV1Container(TypedDict):
    token_metadata: TokenMetadata
    amount: int


def get_listing_metadata(
    move_resource_type: str, data: dict
) -> Optional[ListingMetadata]:
    if move_resource_type != f"{MARKETPLACE_V2_ADDRESS}::listing::Listing":
        return None

    return {
        "seller": standardize_address(data["seller"]),
        "token_address": standardize_address(data["object"]["inner"]),
    }


def get_fixed_priced_listing(
    move_resource_type: str, data: dict
) -> Optional[FixedPriceListing]:
    if (
        f"{MARKETPLACE_V2_ADDRESS}::coin_listing::FixedPriceListing"
        not in move_resource_type
    ):
        return None

    return {"price": data["price"]}


def get_listing_token_v1_container(
    move_resource_type: str, data: dict
) -> Optional[ListingTokenV1Container]:
    if move_resource_type != f"{MARKETPLACE_V2_ADDRESS}::listing::TokenV1Container":
        return None

    token = data.get("token", {})
    amount = token["amount"]
    property_version = token.get("id", {}).get("property_version")
    token_data_id_struct = token.get("id", {}).get("token_data_id", {})
    token_data_id_type = TokenDataIdType(
        token_data_id_struct["creator"],
        token_data_id_struct["collection"],
        token_data_id_struct["name"],
    )

    return {
        "token_metadata": {
            "collection_id": token_data_id_type.get_collection_data_id_hash(),
            "token_data_id": token_data_id_type.to_hash(),
            "creator_address": token_data_id_type.get_creator(),
            "collection_name": token_data_id_type.get_collection_trunc(),
            "token_name": token_data_id_type.get_name_trunc(),
            "property_version": property_version,
            "token_standard": TokenStandard.V1,
        },
        "amount": amount,
    }
