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
) -> List[nft_marketplace_activities_pb2.NFTMarketplaceActivityRow]:
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
            float(
                data.get("price")
                or data.get("coin_per_token")
                or data.get("coin_amount")
                or 0
            )
        ) / 10**8

        # Token amount parsing
        token_amount = int(data.get("token_amount", 1))

        # Buyer and seller parsing
        buyer = str(data.get("buyer", None))
        seller = str(data.get("token_owner", None))

        activity = nft_marketplace_activities_pb2.NFTMarketplaceActivityRow(
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
            collection_id=None,
            price=price,
            amount=token_amount,
            buyer=standardize_address(buyer),
            seller=standardize_address(seller),
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
