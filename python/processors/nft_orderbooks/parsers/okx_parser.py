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

OKX_MARKETPLACE_EVENT_TYPES = set(
    [
        "okx_listing_utils::ListingEvent",
        "okx_bid_utils::OrderExecutedEvent",
        "okx_listing_utils::CancelListingEvent",
    ]
)


def parse_topaz_marketplace_events(
    transaction: transaction_pb2.Transaction,
) -> List[nft_marketplace_activities_pb2.NFTMarketplaceActivityRow]:
    topaz_raw_events = get_marketplace_events(transaction, MarketplaceName.TOPAZ)
    nft_activities = []

    for event in topaz_raw_events:
        # Readable transaction event type
        display_event_type = event.event_type.replace(
            "0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43::", ""
        ).replace("<0x1::aptos_coin::AptosCoin>", "")
        if display_event_type not in OKX_MARKETPLACE_EVENT_TYPES:
            continue

        data = json.loads(event.json_data)

        # Collection, token, and creator parsing
        token_data_id_struct = data.get("token_id", {}).get("token_data_id", {})
        collection = token_data_id_struct.get("collection", None)
        token_name = token_data_id_struct.get("name", None)
        creator = token_data_id_struct.get("creator", None)

        token_data_id_type = TokenDataIdType(creator, collection, token_name)
        token_name_trunc = token_data_id_type.get_name_trunc()
        token_data_id = token_data_id_type.to_hash()

        # Price parsing
        executed_price = float(data.get("executed_price", 0)) / 10**8
        list_price = float(data.get("min_price", 0)) / 10**8
        price = executed_price or list_price

        # Amount parsing
        amount = int(data.get("amount", 0))

        # Buyer and seller parsing
        buyer = str(data.get("buyer", None))
        seller = str(data.get("id", {}).get("addr", None))

        activity = nft_marketplace_activities_pb2.NFTMarketplaceActivityRow(
            transaction_version=event.transaction_version,
            event_index=event.event_index,
            event_type=display_event_type,
            standard_event_type=standardize_marketplace_event_type(
                display_event_type
            ).value,
            creator_address=standardize_address(creator),
            collection=token_data_id_type.get_collection_trunc(),
            token_name=token_name_trunc,
            token_data_id=token_data_id,
            collection_id=token_data_id_type.get_collection_data_id_hash(),
            price=price,
            amount=amount,
            buyer=standardize_address(buyer),
            seller=standardize_address(seller),
            json_data=event.json_data,
            marketplace=MarketplaceName.OKX.value,
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
        case "okx_bid_utils::OrderExecutedEvent":
            return StandardMarketplaceEventType.LISTING_FILLED
        case "okx_listing_utils::ListingEvent",:
            return StandardMarketplaceEventType.LISTING_PLACE
        case "okx_listing_utils::CancelListingEvent":
            return StandardMarketplaceEventType.LISTING_CANCEL
        case _:
            return StandardMarketplaceEventType.UNKNOWN
