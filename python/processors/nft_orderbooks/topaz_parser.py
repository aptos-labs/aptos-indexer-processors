from aptos.transaction.testing1.v1 import transaction_pb2
import json
import hashlib
from typing import List
from nft_orderbooks_parser import (
    getMarketplaceEvents,
    MarketplaceName,
    MarketplaceEvent,
    ParsedMarketplaceEvent,
)

topazMarketplaceEventTypes = set(
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


def parseTopazMarketplaceEvents(
    transaction: transaction_pb2.Transaction,
) -> List[ParsedMarketplaceEvent]:
    topazEvents = getMarketplaceEvents(transaction, MarketplaceName.TOPAZ)
    parsedEvents = []

    for event in topazEvents:
        # Readable transaction event type
        type2 = event.type.replace(
            "0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2::", ""
        )
        if type2 not in topazMarketplaceEventTypes:
            continue

        data = json.loads(event.jsonData)

        # Collection, token, and creator parsing
        tokenDataID = data.get("token_id", []).get("token_data_id", [])
        collection = tokenDataID.get("collection") | data.get("collection_name") | ""
        token = tokenDataID.get("token") | ""
        creator = tokenDataID.get("creator") | data.get("creator") | ""
        tokenID = hashlib.sha256((f"{creator}{collection}{token}").encode()).hexdigest()

        # Price parsing
        price = (
            float(
                data.get("price") | data.get("min_price") | data.get("coin_amount") | 0
            )
            / 10**8
        )

        # Amount parsing
        amount = float(data.get("amount") | data.get("token_amount") | 0)

        # Buyer and seller parsing
        buyer = data.get("buyer") | data.get("token_buyer") | ""
        seller = data.get("seller") | ""

        # bid_id is a unique identifier assigned by Topaz to each bid on an item. there are no batch bids.
        bidID = data.get("bid_id", "")
        if "CollectionBidEvent" in type2:
            bidID = "topaz_collection_bid_id:" + bidID
        else:
            bidID = "topaz_bid_id:" + bidID

        # listing_id is a unique identifier assigned by Topaz to each listing on an item. each distinct listing within a batch listing has it"s own bid_id
        listingID = "topaz_listing_id:" + data.get("listing_id", "")

        parsedEvents.append(
            ParsedMarketplaceEvent(
                jsonData=event.jsonData,
                marketplace=str(MarketplaceName.TOPAZ),
                contractAddr=event.contractAddr,
                func=event.func,
                transactionVersion=event.transactionVersion,
                transactionBlockHeight=event.transactionBlockHeight,
                type2=type2,
                creator=creator,
                collection=collection,
                token=token,
                tokenID=tokenID,
                bidID=bidID,
                listingID=listingID,
                price=price,
                amount=amount,
                buyer=buyer,
                seller=seller,
            )
        )

    return parsedEvents
