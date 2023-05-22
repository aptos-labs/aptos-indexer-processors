from aptos.transaction.testing1.v1 import transaction_pb2
import json
from typing import List
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    get_marketplace_events,
    MarketplaceName,
    StandardMarketplaceEventType,
)
from processors.nft_orderbooks.models.proto_autogen import (
    nft_marketplace_activities_pb2,
)
from utils.token_utils import CollectionDataIdType, TokenDataIdType, standardize_address

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


def parse_marketplace_events(
    transaction: transaction_pb2.Transaction,
) -> List[nft_marketplace_activities_pb2.NFTMarketplaceActivityRow]:
    topaz_raw_events = get_marketplace_events(transaction, MarketplaceName.TOPAZ)
    nft_activities = []

    for event in topaz_raw_events:
        # Readable transaction event type
        display_event_type = event.event_type.replace(
            "0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2::", ""
        )
        if display_event_type not in TOPAZ_MARKETPLACE_EVENT_TYPES:
            continue

        data = json.loads(event.json_data)

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
        price = (
            float(
                data.get("price")
                or data.get("min_price")
                or data.get("coin_amount")
                or 0
            )
            / 10**8
        )

        # Amount parsing
        amount = int(data.get("amount") or data.get("token_amount") or 0)

        # Buyer and seller parsing
        buyer = data.get("buyer", None) or data.get("token_buyer", None)
        seller = data.get("seller", None)

        activity = nft_marketplace_activities_pb2.NFTMarketplaceActivityRow(
            transaction_version=event.transaction_version,
            event_index=event.event_index,
            event_type=display_event_type,
            standard_event_type=standardize_marketplace_event_type(
                display_event_type
            ).value,
            creator_address=standardize_address(creator),
            collection=collection_data_id_type.get_name_trunc(),
            token_name=token_name_trunc,
            token_data_id=token_data_id,
            collection_id=collection_data_id_type.to_hash(),
            price=price,
            amount=amount,
            buyer=standardize_address(buyer) if buyer != None else None,
            seller=standardize_address(seller) if seller != None else None,
            json_data=event.json_data,
            marketplace=MarketplaceName.TOPAZ.value,
            contract_address=event.contract_address,
            entry_function_id_str=event.entry_function_name,
            transaction_timestamp=event.transaction_timestamp,
        )

        nft_activities.append(activity)

    return nft_activities


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
