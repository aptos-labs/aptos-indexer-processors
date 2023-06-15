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
from utils import transaction_utils
from utils.token_utils import TokenDataIdType, standardize_address

SOUFFLE_MARKETPLACE_EVENT_TYPES = set(
    [
        # these are trade-related events
        # trade-related events are by nature also orderbook-related events
        "FixedPriceMarket::BuyTokenEvent",
        # these are orderbook-related events
        "FixedPriceMarket::ListTokenEvent",
        "FixedPriceMarket::CancelListTokenEvent",
    ]
)


def parse_marketplace_events(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceEvent]:
    souffle_raw_events = get_marketplace_events(transaction, MarketplaceName.SOUFFLE)
    nft_activities = []

    for event in souffle_raw_events:
        # Readable transaction event type
        display_event_type = event.event_type.replace(
            "0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4::", ""
        ).replace("<0x1::aptos_coin::AptosCoin>", "")

        if display_event_type not in SOUFFLE_MARKETPLACE_EVENT_TYPES:
            continue

        data = json.loads(event.json_data)

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
        price = (float(price) / 10**8) if price != None else None

        # Token amount parsing
        token_amount = int(data.get("token_amount", 1))

        standard_event_type = standardize_marketplace_event_type(display_event_type)

        # Buyer and seller parsing
        buyer = data.get("buyer", None)
        if standard_event_type == StandardMarketplaceEventType.LISTING_CANCEL:
            user_transaction = transaction_utils.get_user_transaction(transaction)
            seller = (
                transaction_utils.get_sender(user_transaction)
                if user_transaction
                else None
            )
        else:
            seller = data.get("token_owner", None)

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
            token_amount=token_amount,
            buyer=standardize_address(buyer) if buyer else None,
            seller=standardize_address(seller) if seller else None,
            json_data=event.json_data,
            marketplace=MarketplaceName.SOUFFLE.value,
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
        case "FixedPriceMarket::BuyTokenEvent":
            return StandardMarketplaceEventType.LISTING_FILLED
        case "FixedPriceMarket::ListTokenEvent":
            return StandardMarketplaceEventType.LISTING_PLACE
        case "FixedPriceMarket::CancelListTokenEvent":
            return StandardMarketplaceEventType.LISTING_CANCEL
        case _:
            return StandardMarketplaceEventType.UNKNOWN
