from typing import Optional
from typing_extensions import TypedDict
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


# Listing models
class FixedPriceListing(TypedDict):
    price: int


class ListingMetadata(TypedDict):
    seller: str
    fee_schedule_id: str
    # Either the token v2 address or the token v1 container address
    token_address: str


class ListingTokenV1Container(TypedDict):
    token_metadata: TokenMetadata
    amount: int


def get_listing_metadata(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[ListingMetadata]:
    if move_resource_type != f"{marketplace_contract_address}::listing::Listing":
        return None

    return {
        "seller": standardize_address(data["seller"]),
        "fee_schedule_id": standardize_address(data["fee_schedule"]["inner"]),
        "token_address": standardize_address(data["object"]["inner"]),
    }


def get_fixed_priced_listing(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[FixedPriceListing]:
    if (
        f"{marketplace_contract_address}::coin_listing::FixedPriceListing"
        not in move_resource_type
    ):
        return None

    return {"price": data["price"]}


def get_listing_token_v1_container(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[ListingTokenV1Container]:
    if (
        move_resource_type
        != f"{marketplace_contract_address}::listing::TokenV1Container"
    ):
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


# Token offer models and helpers
class TokenOfferMetadata(TypedDict):
    expiration_time: int
    price: int
    fee_schedule_id: str


class TokenOfferV2(TypedDict):
    token_address: str


class TokenOfferV1(TypedDict):
    token_metadata: TokenMetadata


def get_token_offer_metadata(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[TokenOfferMetadata]:
    if move_resource_type != f"{marketplace_contract_address}::token_offer::TokenOffer":
        return None

    return {
        "expiration_time": data["expiration_time"],
        "price": data["item_price"],
        "fee_schedule_id": standardize_address(data["fee_schedule"]["inner"]),
    }


def get_token_offer_v2(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[TokenOfferV2]:
    if (
        move_resource_type
        != f"{marketplace_contract_address}::token_offer::TokenOfferTokenV2"
    ):
        return None

    return {
        "token_address": standardize_address(data["token"]["inner"]),
    }


def get_token_offer_v1(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[TokenOfferV1]:
    if (
        move_resource_type
        != f"{marketplace_contract_address}::token_offer::TokenOfferTokenV1"
    ):
        return None

    property_version = data.get("property_version")
    token_data_id_type = TokenDataIdType(
        data["creator_address"],
        data["collection_name"],
        data["token_name"],
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
    }


# Collection offer models and helpers


class CollectionOfferMetadata(TypedDict):
    expiration_time: int
    item_price: int
    remaining_token_amount: int
    fee_schedule_id: str


class CollectionOfferV1(TypedDict):
    collection_metadata: CollectionMetadata


class CollectionOfferV2(TypedDict):
    collection_address: str


class CollectionOfferEventMetadata(TypedDict):
    collection_offer_id: str
    collection_metadata: CollectionMetadata
    item_price: int
    buyer: str
    fee_schedule_id: str


def get_collection_offer_metadata(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[CollectionOfferMetadata]:
    if (
        move_resource_type
        != f"{marketplace_contract_address}::collection_offer::CollectionOffer"
    ):
        return None

    return {
        "expiration_time": data["expiration_time"],
        "item_price": data["item_price"],
        "remaining_token_amount": data["remaining"],
        "fee_schedule_id": standardize_address(data["fee_schedule"]["inner"]),
    }


def get_collection_offer_v1(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[CollectionOfferV1]:
    if (
        move_resource_type
        != f"{marketplace_contract_address}::collection_offer::CollectionOfferTokenV1"
    ):
        return None

    collection_data_id_type = CollectionDataIdType(
        data["creator_address"],
        data["collection_name"],
    )

    return {
        "collection_metadata": {
            "collection_id": collection_data_id_type.to_hash(),
            "creator_address": collection_data_id_type.get_creator(),
            "collection_name": collection_data_id_type.get_name_trunc(),
            "token_standard": TokenStandard.V1,
        },
    }


def get_collection_offer_v2(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[CollectionOfferV2]:
    if (
        move_resource_type
        != f"{marketplace_contract_address}::collection_offer::CollectionOfferTokenV2"
    ):
        return None

    return {
        "collection_address": standardize_address(data["collection"]["inner"]),
    }


# Auction models and helpers
class AuctionListing(TypedDict):
    auction_end_time: int
    starting_bid_price: int
    current_bid_price: Optional[int]
    current_bidder: Optional[str]
    buy_it_now_price: Optional[int]


def get_auction_listing(
    move_resource_type: str, data: dict, marketplace_contract_address: str
) -> Optional[AuctionListing]:
    if (
        f"{marketplace_contract_address}::coin_listing::AuctionListing"
        not in move_resource_type
    ):
        return None

    maybe_buy_it_now_price = data.get("buy_it_now_price", {}).get("vec", [])
    buy_it_now_price = maybe_buy_it_now_price[0] if maybe_buy_it_now_price else None

    current_bid_price = None
    current_bidder = None
    maybe_current_bid = data.get("current_bid", {}).get("vec", [])
    if maybe_current_bid:
        current_bid = maybe_current_bid[0]
        current_bid_price = current_bid["coins"]["value"]
        current_bidder = standardize_address(current_bid["bidder"])

    return {
        "auction_end_time": data["auction_end_time"],
        "starting_bid_price": data["starting_bid"],
        "current_bid_price": current_bid_price,
        "current_bidder": current_bidder,
        "buy_it_now_price": buy_it_now_price,
    }
