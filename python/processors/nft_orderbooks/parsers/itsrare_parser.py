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

ITSRARE_MARKETPLACE_EVENT_TYPES = set(
    [
        # these are trade-related events
        # trade-related events are by nature also orderbook-related events
        "Events::BuyEvent",
        # these are orderbook-related events
        "Events::ListEvent",
        "Events::DelistEvent"
        # # unrelated to order book; these are filtered out (included here in comments for documentation)
        # ,token_coin_swap::TokenListingEvent # redundant with events::ListEvent
        # ,token_coin_swap::TokenSwapEvent    # redundant with events::BuyEvent
    ]
)


def parse_marketplace_events(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceEvent]:
    topaz_raw_events = get_marketplace_events(transaction, MarketplaceName.ITSRARE)
    nft_activities = []

    for event in topaz_raw_events:
        # Readable transaction event type
        display_event_type = event.event_type.replace(
            "0x143f6a7a07c76eae1fc9ea030dbd9be3c2d46e538c7ad61fe64529218ff44bc4::", ""
        )
        if display_event_type not in ITSRARE_MARKETPLACE_EVENT_TYPES:
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
        amount = int(data.get("amount", 1))

        # Buyer and seller parsing
        buyer = data.get("buyer", None)
        seller = data.get("seller", None) or data.get("lister", None)

        activity = NFTMarketplaceEvent(
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
            token_amount=amount,
            buyer=standardize_address(buyer) if buyer else None,
            seller=standardize_address(seller) if seller else None,
            json_data=event.json_data,
            marketplace=MarketplaceName.ITSRARE.value,
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
        case "Events::BuyEvent":
            return StandardMarketplaceEventType.LISTING_FILLED
        case "Events::ListEvent":
            return StandardMarketplaceEventType.LISTING_PLACE
        case "Events::DelistEvent":
            return StandardMarketplaceEventType.LISTING_CANCEL
        case _:
            return StandardMarketplaceEventType.UNKNOWN
