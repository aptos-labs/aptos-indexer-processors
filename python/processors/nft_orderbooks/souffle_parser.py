# from aptos.transaction.testing1.v1 import transaction_pb2
# import json
# import hashlib
# from typing import List
# from nft_orderbooks_parser import (
#     getMarketplaceEvents,
#     MarketplaceName,
#     MarketplaceEvent,
#     ParsedMarketplaceEvent,
# )

# souffleMarketplaceEventTypes = set(
#     [
#         # these are trade-related events
#         # trade-related events are by nature also orderbook-related events
#         "FixedPriceMarket::BuyTokenEvent",
#         # these are orderbook-related events
#         "FixedPriceMarket::ListTokenEvent",
#         "FixedPriceMarket::CancelListTokenEvent",
#     ]
# )


# def parseSouffleMarketplaceEvents(
#     transaction: transaction_pb2.Transaction,
# ) -> List[MarketplaceEvent]:
#     souffleEvents = getMarketplaceEvents(transaction, MarketplaceName.SOUFFLE)
#     parsedEvents = []

#     for event in souffleEvents:
#         # Readable transaction event type
#         type2 = event.type.replace(
#             "0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4::", ""
#         ).replace("<0x1::aptos_coin::AptosCoin>", "")

#         if type2 not in souffleMarketplaceEventTypes:
#             continue

#         data = json.loads(event.jsonData)

#         # Collection, token, and creator parsing
#         tokenDataID = data.get("token_id", []).get("token_data_id", [])
#         collection = tokenDataID.get("collection") | ""
#         token = tokenDataID.get("name") | ""
#         creator = tokenDataID.get("creator") | ""

#         # Create my own token hash for partitioning. this will be used to propagate listing hashes
#         tokenID = hashlib.sha256((f"{creator}{collection}{token}").encode()).hexdigest()

#         # Price parsing and consolidation
#         price = (
#             float(
#                 data.get("price")
#                 | data.get("coin_per_token")
#                 | data.get("coin_amount")
#                 | 0
#             )
#             / 10**8
#         )

#         # Amount parsing
#         amount = float(data.get("token_amount") | 1)

#         # Souffle does not support bids
#         bidID = ""

#         # if event is a token listing event, assign a unique listing hash
#         # this will be propagated out to buy and cancel events pertaining to the listing
#         if "::ListTokenEvent" in type2:
#             listingIDRaw = hashlib.sha256(
#                 (f"{creator}{collection}{token}{transaction.version}").encode()
#             ).hexdigest()
#         else:
#             listIDRaw = None

#         # Get all events with same tokenID
#         # Order by transaction version (descending), then type2 like Cancel event (descending)
#         # Get the last event in the list's listingIDRaw and priceListing (if applicable)
#         # if type2 in ['FixedPriceMarket::BuyTokenEvent', 'FixedPriceMarket::CancelListTokenEvent']:

#         # consolidation of price on only listing events
#         # this will be propagated out to buy and cancel events pertaining to the listing
#         # if "::ListTokenEvent" in type2:
#         #     priceListing = price
#         # else:
#         #     priceListing = None

#         # Buyer, seller parsing
#         buyer = data.get("buyer") | ""
#         seller = data.get("token_owner") | ""
