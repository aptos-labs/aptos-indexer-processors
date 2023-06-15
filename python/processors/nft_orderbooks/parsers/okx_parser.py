from aptos.transaction.v1 import transaction_pb2
import json
import re
from typing import Dict, List, Optional, TypedDict
from processors.nft_orderbooks.nft_marketplace_enums import (
    MarketplaceName,
    StandardMarketplaceEventType,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from utils.token_utils import CollectionDataIdType, TokenDataIdType, standardize_address
from utils import event_utils, general_utils, transaction_utils

OKX_MARKETPLACE_EVENT_TYPES = set(
    [
        "okx_listing_utils::ListingEvent",
        "okx_bid_utils::OrderExecutedEvent",
        "okx_listing_utils::CancelListingEvent",
    ]
)


def parse_marketplace_events(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceEvent]:
    user_transaction = transaction_utils.get_user_transaction(transaction)

    if user_transaction is None:
        return []

    move_module = transaction_utils.get_move_module(user_transaction)
    contract_address = move_module.address
    if (
        contract_address
        != "0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43"
    ):
        return []

    nft_activities = []
    entry_function_payload = transaction_utils.get_entry_function_payload(
        user_transaction
    )
    entry_function = entry_function_payload.function
    entry_function_name = f"{entry_function.module.name}::{entry_function.name}"

    for event_index, event in enumerate(user_transaction.events):
        # Readable transaction event type
        display_event_type = event.type_str.replace(
            "0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43::", ""
        ).replace("<0x1::aptos_coin::AptosCoin>", "")
        if display_event_type not in OKX_MARKETPLACE_EVENT_TYPES:
            continue

        data = json.loads(event.data)

        # Get token metadata
        token_data_id_type = None
        collection_trunc = None
        token_name_trunc = None
        collection_data_id = None
        token_data_id = None
        creator = None

        standard_marketplace_event_type = standardize_marketplace_event_type(
            display_event_type
        )

        if (
            standard_marketplace_event_type
            == StandardMarketplaceEventType.LISTING_FILLED
        ):
            # Token metadata for listing fill event exist in the deposit events
            # in the same transaction
            deposit_events = get_token_data_from_deposit_events(user_transaction)
            account_address = event_utils.get_account_address(event)
            if standardize_address(account_address) in deposit_events:
                token_data_id_type = deposit_events[
                    standardize_address(account_address)
                ]
        elif (
            standard_marketplace_event_type
            == StandardMarketplaceEventType.LISTING_PLACE
        ):
            # Token metadata for listing place event exist in the event itself
            token_data_id_struct = data.get("token_id", {}).get("token_data_id", {})
            collection = token_data_id_struct.get("collection", None)
            token = token_data_id_struct.get("name", None)
            creator = token_data_id_struct.get("creator", None)
            token_data_id_type = TokenDataIdType(creator, collection, token)

        if token_data_id_type != None:
            collection_trunc = token_data_id_type.get_collection_trunc()
            token_name_trunc = token_data_id_type.get_name_trunc()
            collection_data_id = token_data_id_type.get_collection_data_id_hash()
            token_data_id = token_data_id_type.to_hash()
            creator = token_data_id_type.creator

        # Price parsing
        executed_price = float(data.get("executed_price", 0)) / 10**8
        list_price = float(data.get("min_price", 0)) / 10**8
        price = executed_price or list_price

        # Amount parsing
        amount = int(data.get("amount", 0))

        # Buyer and seller parsing
        buyer = data.get("buyer", None)
        seller = data.get("id", {}).get("addr", None)

        activity = NFTMarketplaceEvent(
            transaction_version=transaction.version,
            event_index=event_index,
            event_type=display_event_type,
            standard_event_type=standardize_marketplace_event_type(
                display_event_type
            ).value,
            creator_address=standardize_address(creator) if creator else None,
            collection=collection_trunc,
            token_name=token_name_trunc,
            token_data_id=token_data_id,
            collection_id=collection_data_id,
            price=price,
            token_amount=amount,
            buyer=standardize_address(buyer) if buyer else None,
            seller=standardize_address(seller) if seller else None,
            json_data=event.data,
            marketplace=MarketplaceName.OKX.value,
            contract_address=contract_address,
            entry_function_id_str=entry_function_name,
            transaction_timestamp=general_utils.convert_pb_timestamp_to_datetime(
                transaction.timestamp
            ),
        )

        nft_activities.append(activity)

    return nft_activities


def get_token_data_from_deposit_events(user_transaction) -> Dict[str, TokenDataIdType]:
    # Extract deposit events, which contain token metadata
    deposit_events: Dict[str, TokenDataIdType] = {}
    for event in user_transaction.events:
        if event.type_str != "0x3::token::DepositEvent":
            continue
        account_address = standardize_address(event_utils.get_account_address(event))
        data = json.loads(event.data)

        # Collection, token, and creator parsing
        token_data_id_struct = data.get("id", {}).get("token_data_id", {})
        collection = token_data_id_struct.get("collection", None)
        creator = token_data_id_struct.get("creator", None)
        token_name = token_data_id_struct.get("name")
        token_data_id_type = TokenDataIdType(creator, collection, token_name)

        deposit_events[account_address] = token_data_id_type
    return deposit_events


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
