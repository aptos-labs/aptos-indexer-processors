from aptos.transaction.v1 import transaction_pb2
import json
from typing import List
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    get_marketplace_events,
    MarketplaceName,
    StandardMarketplaceEventType,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from utils.token_utils import CollectionDataIdType, TokenDataIdType, standardize_address

TOPAZ_MARKETPLACE_EVENT_TYPES = set(
    [
        # these are trade-related events
        "marketplaceV2::BuyEvent",  # purchase
        "offer_lib::AcceptOfferCollectionEvent",  # accept global bid
        "offer_lib::AcceptOfferEvent",  # accept bid
        # these are orderbook related events
        "offer_lib::OfferCollectionEvent",  # global bid
        "offer_lib::CancelOfferCollectionEvent",  # cancelled global bid
        "offer_lib::OfferEvent",  # bid
        "offer_lib::CancelOfferEvent",  # cancelled bid
        "marketplaceV2::ListEvent",  # listing
        "marketplaceV2::ChangePriceEvent",  # change price of listing
        "marketplaceV2::DelistEvent"  # cancel listing
        # # all auctions on bluemove look unsuccessful
        # ,'marketplaceV2::AuctionEvent'
        # ,'marketplaceV2::BidEvent' # bids on unsuccessful auctions
        # # unrelated to order book; these are filtered out (included here in comments for documentation)
        # ,'marketplaceV2::ClaimTokenEvent'
        # ,'marketplaceV2::ClaimCoinsEvent'
        # ,'offer_lib::ClaimTokenOffer'
        # ,'offer_lib::ClaimTokenOfferCollectionEvent'
    ]
)


def parse_marketplace_events(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceEvent]:
    topaz_raw_events = get_marketplace_events(transaction, MarketplaceName.BLUEMOVE)
    nft_activities = []

    for event in topaz_raw_events:
        # Readable transaction event type
        display_event_type = event.event_type.replace(
            "0xd1fd99c1944b84d1670a2536417e997864ad12303d19eac725891691b04d614e::", ""
        )
        if display_event_type not in TOPAZ_MARKETPLACE_EVENT_TYPES:
            continue

        data = json.loads(event.json_data)
        standard_marketplace_event_type = standardize_marketplace_event_type(
            display_event_type
        )

        # Collection, token, and creator parsing
        token_data_id_struct1 = data.get("token_id", {}).get("token_data_id", {})
        token_data_id_struct2 = data.get("id", {}).get("token_data_id", {})
        offer_collection_item_struct = data.get("offer_collection_item", {})

        collection = str(
            token_data_id_struct1.get("collection")
            or token_data_id_struct2.get("collection")
            or offer_collection_item_struct.get("collection_name")
            or None
        )
        token_name = str(
            token_data_id_struct1.get("name")
            or token_data_id_struct2.get("name")
            or None
        )
        creator = str(
            token_data_id_struct1.get("creator")
            or token_data_id_struct2.get("creator")
            or offer_collection_item_struct.get("creator_address")
            or None
        )

        token_name_trunc = None
        token_data_id = None
        collection_data_id_type = CollectionDataIdType(creator, collection)
        if token_name != None:
            token_data_id_type = TokenDataIdType(creator, collection, token_name)
            token_name_trunc = token_data_id_type.get_name_trunc()
            token_data_id = token_data_id_type.to_hash()

        # Price parsing
        bid_amount = float(data.get("bid", 0)) / 10**8
        min_selling_price = float(data.get("min_selling_price", 0)) / 10**8
        amount_per_item = (
            float(offer_collection_item_struct.get("amount_per_item", 0)) / 10**8
        )
        price_amount = float(data.get("amount", 0)) / 10**8
        price = bid_amount or min_selling_price or price_amount or amount_per_item

        # Amount parsing
        quantity_offer_items = int(
            offer_collection_item_struct.get("quantity_offer_items", 0)
        )
        quantity_cancel_items = int(data.get("quantity_cancel_items", 0))
        amount = int(
            quantity_cancel_items
            or (
                quantity_offer_items
                if standard_marketplace_event_type
                == StandardMarketplaceEventType.BID_PLACE
                else None
            )
            or 1
        )

        # Buyer and seller parsing
        buyer_bidder = str(data.get("bider_address", None))
        buyer_address = str(data.get("buyer_address", None))
        offerer1 = str(offer_collection_item_struct.get("offerer", None))
        offerer2 = str(data.get("offerer", None))
        buyer = buyer_bidder or offerer1 or offerer2 or buyer_address

        owner_address = str(data.get("owner_address", None))
        seller_address = str(data.get("seller_address", None))
        seller_accepted_token_bids = str(data.get("owner_token", None))
        can_claim_tokens_data = offer_collection_item_struct.get(
            "can_claim_tokens", {}
        ).get("data", [])
        seller_accepted_collection_bids = (
            can_claim_tokens_data[0] if len(can_claim_tokens_data) > 0 else {}
        ).get("value", None)
        seller = (
            seller_address
            or owner_address
            or seller_accepted_token_bids
            or seller_accepted_collection_bids
        )

        activity = NFTMarketplaceEvent(
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
            buyer=standardize_address(buyer) if buyer else None,
            seller=standardize_address(seller) if seller else None,
            json_data=event.json_data,
            marketplace=MarketplaceName.BLUEMOVE.value,
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
        case "marketplaceV2::BuyEvent":
            return StandardMarketplaceEventType.LISTING_FILLED
        case "marketplaceV2::ListEvent":
            return StandardMarketplaceEventType.LISTING_PLACE
        case "marketplaceV2::DelistEvent":
            return StandardMarketplaceEventType.LISTING_CANCEL
        case "marketplaceV2::ChangePriceEvent":
            return StandardMarketplaceEventType.LISTING_CHANGE
        case "offer_lib::AcceptOfferCollectionEvent" | "offer_lib::AcceptOfferEvent":
            return StandardMarketplaceEventType.BID_FILLED
        case "offer_lib::OfferCollectionEvent" | "offer_lib::OfferEvent":
            return StandardMarketplaceEventType.BID_PLACE
        case "offer_lib::CancelOfferCollectionEvent" | "offer_lib::CancelOfferEvent":
            return StandardMarketplaceEventType.BID_CANCEL
        case _:
            return StandardMarketplaceEventType.UNKNOWN
