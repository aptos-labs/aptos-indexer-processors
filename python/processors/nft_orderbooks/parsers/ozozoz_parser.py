import re

from aptos.transaction.v1 import transaction_pb2
import json
from typing import List
from processors.nft_orderbooks.nft_marketplace_enums import (
    MarketplaceName,
    StandardMarketplaceEventType,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    get_marketplace_events,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from utils.token_utils import CollectionDataIdType, TokenDataIdType, standardize_address
from utils import transaction_utils

OZOZOZ_MARKETPLACE_EVENT_TYPES = set(
    [
        "OzozozMarketplace::UpdatePriceEvent",
        "OzozozMarketplace::BuyEvent",
        "OzozozMarketplace::ListEvent",
        "OzozozMarketplace::DelistEvent",
    ]
)


def parse_marketplace_events(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceEvent]:
    ozozoz_raw_events = get_marketplace_events(transaction, MarketplaceName.OZOZOZ)
    nft_activities = []

    for event in ozozoz_raw_events:
        # Readable transaction event type
        display_event_type = event.event_type.replace(
            "0xded0c1249b522cecb11276d2fad03e6635507438fef042abeea3097846090bcd::", ""
        )
        if display_event_type not in OZOZOZ_MARKETPLACE_EVENT_TYPES:
            continue

        data = json.loads(event.json_data)

        # Collection, token, and creator parsing
        token_data_id_struct = data.get("tokenId", {}).get("token_data_id", {})
        collection = token_data_id_struct.get("collection", None)
        token_name = token_data_id_struct.get("name", None)
        creator = token_data_id_struct.get("creator", None) or data.get("creator", None)
        token_data_id_type = TokenDataIdType(creator, collection, token_name)

        # Price parsing
        price = float(data.get("price") or 0) / 10**8

        # Amount parsing
        amount = int(data.get("amount") or 0)

        # Buyer and seller parsing
        user = data.get("user", None)
        seller_address = data.get("sellerAddress", None)

        standard_event_type = standardize_marketplace_event_type(display_event_type)
        if standard_event_type == StandardMarketplaceEventType.LISTING_FILLED:
            buyer = user
            seller = seller_address
        else:
            buyer = None
            seller = user

        activity = NFTMarketplaceEvent(
            transaction_version=event.transaction_version,
            event_index=event.event_index,
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
            token_amount=amount,
            buyer=standardize_address(buyer) if buyer else None,
            seller=standardize_address(seller),
            json_data=event.json_data,
            marketplace=MarketplaceName.OZOZOZ.value,
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
        case "OzozozMarketplace::UpdatePriceEvent":
            return StandardMarketplaceEventType.LISTING_CHANGE
        case "OzozozMarketplace::BuyEvent":
            return StandardMarketplaceEventType.LISTING_FILLED
        case "OzozozMarketplace::ListEvent":
            return StandardMarketplaceEventType.LISTING_PLACE
        case "OzozozMarketplace::DelistEvent":
            return StandardMarketplaceEventType.LISTING_CANCEL
        case _:
            return StandardMarketplaceEventType.UNKNOWN
