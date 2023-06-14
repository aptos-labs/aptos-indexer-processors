from aptos.transaction.v1 import transaction_pb2
import json
from typing import List
from processors.nft_orderbooks.nft_marketplace_enums import (
    MarketplaceName,
    StandardMarketplaceEventType,
    ListingTableMetadata,
    BidMetadata,
    CollectionBidMetadata,
)
from processors.nft_orderbooks.nft_marketplace_constants import (
    BLUEMOVE_MARKETPLACE_EVENT_TYPES,
    BLUEMOVE_LISTINGS_TABLE_HANDLE,
    BLUEMOVE_BIDS_TABLE_HANDLE,
    BLUEMOVE_COLLECTION_BIDS_TABLE_HANDLE,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from processors.nft_orderbooks.models.nft_marketplace_bid_models import (
    CurrentNFTMarketplaceBid,
    NFTMarketplaceBid,
    CurrentNFTMarketplaceCollectionBid,
    NFTMarketplaceCollectionBid,
)
from processors.nft_orderbooks.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    parse_transaction_metadata,
    lookup_current_listing_in_db,
    lookup_current_bid_in_db,
)
from utils.token_utils import CollectionDataIdType, TokenDataIdType, standardize_address
from utils import event_utils, transaction_utils
from utils.session import Session


def parse_event(
    transaction: transaction_pb2.Transaction,
    event: transaction_pb2.Event,
    event_index: int,
) -> List[NFTMarketplaceEvent]:
    parsed_objs = []

    transaction_metadata = parse_transaction_metadata(transaction)

    # Readable transaction event type
    display_event_type = event_utils.get_event_type_short(event)

    if display_event_type not in BLUEMOVE_MARKETPLACE_EVENT_TYPES:
        return []

    data = json.loads(event.data)
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
        token_data_id_struct1.get("name") or token_data_id_struct2.get("name") or None
    )
    creator = str(
        token_data_id_struct1.get("creator")
        or token_data_id_struct2.get("creator")
        or offer_collection_item_struct.get("creator_address")
        or None
    )

    token_data_id_type = None
    collection_data_id_type = CollectionDataIdType(creator, collection)
    if token_name != None:
        token_data_id_type = TokenDataIdType(creator, collection, token_name)

    # Price parsing
    bid_amount = data.get("bid", None)
    min_selling_price = data.get("min_selling_price", None)
    amount_per_item = offer_collection_item_struct.get("amount_per_item", None)
    price_amount = data.get("amount", None)
    price = bid_amount or min_selling_price or price_amount or amount_per_item
    price = int(price) if price else None

    # Amount parsing
    quantity_offer_items = int(
        offer_collection_item_struct.get("quantity_offer_items", 0)
    )
    quantity_cancel_items = int(data.get("quantity_cancel_items", 0))
    amount = int(
        quantity_cancel_items
        or (
            quantity_offer_items
            if standard_marketplace_event_type == StandardMarketplaceEventType.BID_PLACE
            else None
        )
        or 1
    )

    # Buyer and seller parsing
    buyer_bidder = data.get("bider_address", None)
    buyer_address = data.get("buyer_address", None)
    offerer1 = offer_collection_item_struct.get("offerer", None)
    offerer2 = data.get("offerer", None)
    buyer = buyer_bidder or offerer1 or offerer2 or buyer_address
    buyer = standardize_address(str(buyer)) if buyer else None

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
    seller = standardize_address(seller) if seller else None

    activity = NFTMarketplaceEvent(
        transaction_version=transaction_metadata.transaction_version,
        event_index=event_index,
        event_type=display_event_type,
        standard_event_type=standard_marketplace_event_type.value,
        creator_address=collection_data_id_type.get_creator(),
        collection=collection_data_id_type.get_name_trunc(),
        token_name=token_data_id_type.get_name_trunc() if token_data_id_type else None,
        token_data_id=token_data_id_type.to_hash() if token_data_id_type else None,
        collection_id=collection_data_id_type.to_hash(),
        price=price,
        token_amount=amount,
        buyer=buyer,
        seller=seller,
        json_data=event.data,
        marketplace=MarketplaceName.BLUEMOVE.value,
        contract_address=transaction_metadata.contract_address,
        entry_function_id_str=transaction_metadata.entry_function_id_str_short,
        transaction_timestamp=transaction_metadata.transaction_timestamp,
    )

    parsed_objs.append(activity)

    # Handle listing cancel and fill events
    if (
        standard_marketplace_event_type
        in set(
            [
                StandardMarketplaceEventType.LISTING_CANCEL,
                StandardMarketplaceEventType.LISTING_FILLED,
            ]
        )
        and token_data_id_type is not None
    ):
        # Lookup previous listing price
        previous_price = 0
        previous_seller = None
        previous_listing_metadata = lookup_current_listing_in_db(
            token_data_id_type.to_hash()
        )
        if previous_listing_metadata:
            previous_price = previous_listing_metadata.price
            previous_seller = previous_listing_metadata.seller

        # Get seller from previous listing if listing is filled
        if (
            previous_seller
            and standard_marketplace_event_type
            == StandardMarketplaceEventType.LISTING_FILLED
        ):
            seller = previous_seller

        listing = NFTMarketplaceListing(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=standardize_address(creator),
            token_name=token_data_id_type.get_name_trunc(),
            token_data_id=token_data_id_type.to_hash(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=previous_price,
            token_amount=amount
            * -1,  # Negative if listing is canceled, filled, or edited
            seller=seller,
            buyer=buyer,
            event_type=standard_marketplace_event_type.value,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(listing)

        current_listing = CurrentNFTMarketplaceListing(
            token_data_id=token_data_id_type.to_hash(),
            creator_address=standardize_address(creator),
            token_name=token_data_id_type.get_name_trunc(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=previous_price,
            token_amount=0,  # Zero if listing is canceled or filled
            seller=seller,
            is_deleted=True,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(current_listing)

    # Handle bid cancel events
    elif display_event_type == "offer_lib::CancelOfferEvent" and token_data_id_type:
        # Lookup previous bid price
        old_bid_price = 0
        if token_data_id_type and buyer:
            old_bid_metadata = lookup_current_bid_in_db(
                token_data_id_type.to_hash(), buyer
            )
            if old_bid_metadata:
                old_bid_price = old_bid_metadata.price

        bid = NFTMarketplaceBid(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=token_data_id_type.get_creator(),
            token_name=token_data_id_type.get_name_trunc(),
            token_data_id=token_data_id_type.to_hash(),
            collection=token_data_id_type.get_collection_trunc(),
            collection_id=token_data_id_type.get_collection_data_id_hash(),
            price=old_bid_price,
            token_amount=-1,
            buyer=buyer,
            event_type=standard_marketplace_event_type.value,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_bid = CurrentNFTMarketplaceBid(
            token_data_id=token_data_id_type.to_hash(),
            creator_address=token_data_id_type.get_creator(),
            token_name=token_data_id_type.get_name_trunc(),
            collection=token_data_id_type.get_collection_trunc(),
            collection_id=token_data_id_type.get_collection_data_id_hash(),
            price=old_bid_price,
            token_amount=0,
            buyer=buyer,
            is_deleted=True,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(bid)
        parsed_objs.append(current_bid)

    # Handle collection bid cancel events
    elif display_event_type == "offer_lib::CancelOfferCollectionEvent":
        collection_bid = NFTMarketplaceCollectionBid(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=collection_data_id_type.get_creator(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=int(amount_per_item) if amount_per_item else None,
            token_amount=quantity_cancel_items * -1,
            buyer=buyer,
            event_type=standard_marketplace_event_type.value,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        current_collection_bid = CurrentNFTMarketplaceCollectionBid(
            creator_address=collection_data_id_type.get_creator(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=int(amount_per_item) if amount_per_item else None,
            token_amount=0,
            buyer=buyer,
            is_deleted=True,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        parsed_objs.append(collection_bid)
        parsed_objs.append(current_collection_bid)

    # Handle collection bid place events
    elif display_event_type == "offer_lib::OfferCollectionEvent":
        collection_bid = NFTMarketplaceCollectionBid(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=collection_data_id_type.get_creator(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=int(amount_per_item) if amount_per_item else None,
            token_amount=amount,
            buyer=buyer,
            event_type=standard_marketplace_event_type.value,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        current_collection_bid = CurrentNFTMarketplaceCollectionBid(
            creator_address=collection_data_id_type.get_creator(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=int(amount_per_item) if amount_per_item else None,
            token_amount=amount,
            buyer=buyer,
            is_deleted=False,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        parsed_objs.append(collection_bid)
        parsed_objs.append(current_collection_bid)

    # Handle collection bid fill events
    elif display_event_type == "offer_lib::AcceptOfferCollectionEvent":
        collection_bid = NFTMarketplaceCollectionBid(
            transaction_version=transaction_metadata.transaction_version,
            index=event_index * -1,
            creator_address=collection_data_id_type.get_creator(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=int(amount_per_item) if amount_per_item else None,
            token_amount=-1,
            buyer=buyer,
            seller=seller,
            event_type=standard_marketplace_event_type.value,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        current_collection_bid = CurrentNFTMarketplaceCollectionBid(
            creator_address=collection_data_id_type.get_creator(),
            collection=collection_data_id_type.get_name_trunc(),
            collection_id=collection_data_id_type.to_hash(),
            price=int(amount_per_item) if amount_per_item else None,
            token_amount=quantity_offer_items,
            buyer=buyer,
            is_deleted=quantity_offer_items == 0,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        parsed_objs.append(collection_bid)
        parsed_objs.append(current_collection_bid)

    return parsed_objs


def parse_write_table_item(
    transaction: transaction_pb2.Transaction,
    write_table_item: transaction_pb2.WriteTableItem,
    wsc_index: int,
) -> List[
    NFTMarketplaceEvent
    | NFTMarketplaceListing
    | CurrentNFTMarketplaceListing
    | NFTMarketplaceBid
    | CurrentNFTMarketplaceBid
    | NFTMarketplaceCollectionBid
    | CurrentNFTMarketplaceCollectionBid
]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)

    table_handle = write_table_item.handle

    # Handle listing place and edit events
    if table_handle == BLUEMOVE_LISTINGS_TABLE_HANDLE:
        event_types = set(
            [
                event_utils.get_event_type_short(event)
                for event in user_transaction.events
            ]
        )
        is_edit = "marketplaceV2::ChangePriceEvent" in event_types

        standard_marketplace_event_type = (
            StandardMarketplaceEventType.LISTING_CHANGE
            if is_edit
            else StandardMarketplaceEventType.LISTING_PLACE
        )
        listing_metadata = parse_listing(write_table_item)

        if not listing_metadata:
            return parsed_objs

        # When a listing is edited, this is represetned as a listing delete and
        # listing place in nft_marketplace_listings
        if is_edit and listing_metadata.token_data_id:
            old_listing_metadata = lookup_current_listing_in_db(
                listing_metadata.token_data_id
            )

            if old_listing_metadata:
                delete_listing = NFTMarketplaceListing(
                    transaction_version=transaction_metadata.transaction_version,
                    index=wsc_index * -1,  # To avoid collision with place listing index
                    creator_address=old_listing_metadata.creator_address,
                    token_name=old_listing_metadata.token_name,
                    token_data_id=old_listing_metadata.token_data_id,
                    collection=old_listing_metadata.collection,
                    collection_id=old_listing_metadata.collection_id,
                    price=old_listing_metadata.price,
                    token_amount=old_listing_metadata.amount * -1
                    if old_listing_metadata.amount
                    else None,
                    seller=old_listing_metadata.seller,
                    event_type=standard_marketplace_event_type.value,
                    marketplace=MarketplaceName.BLUEMOVE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
                parsed_objs.append(delete_listing)

        listing = NFTMarketplaceListing(
            transaction_version=transaction_metadata.transaction_version,
            index=wsc_index,
            creator_address=listing_metadata.creator_address,
            token_name=listing_metadata.token_name,
            token_data_id=listing_metadata.token_data_id,
            collection=listing_metadata.collection,
            collection_id=listing_metadata.collection_id,
            price=listing_metadata.price,
            token_amount=listing_metadata.amount,
            seller=listing_metadata.seller,
            event_type=standard_marketplace_event_type.value,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_listing = CurrentNFTMarketplaceListing(
            token_data_id=listing_metadata.token_data_id,
            creator_address=listing_metadata.creator_address,
            token_name=listing_metadata.token_name,
            collection=listing_metadata.collection,
            collection_id=listing_metadata.collection_id,
            price=listing_metadata.price,
            token_amount=listing_metadata.amount,
            seller=listing_metadata.seller,
            is_deleted=False,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(listing)
        parsed_objs.append(current_listing)

    # Handle bid place and fill events
    elif table_handle == BLUEMOVE_BIDS_TABLE_HANDLE:
        bid_data = parse_bid(write_table_item)

        if not bid_data:
            return parsed_objs

        standard_marketplace_event_type = (
            StandardMarketplaceEventType.BID_FILLED
            if bid_data.seller
            else StandardMarketplaceEventType.BID_PLACE
        )

        if standard_marketplace_event_type == StandardMarketplaceEventType.BID_FILLED:
            amount = -1
            is_deleted = True
        else:
            amount = 1
            is_deleted = False

        bid = NFTMarketplaceBid(
            transaction_version=transaction_metadata.transaction_version,
            index=wsc_index,
            creator_address=bid_data.creator_address,
            token_name=bid_data.token_name,
            token_data_id=bid_data.token_data_id,
            collection=bid_data.collection,
            collection_id=bid_data.collection_id,
            price=bid_data.price,
            token_amount=amount,
            buyer=bid_data.buyer,
            seller=bid_data.seller,
            event_type=standard_marketplace_event_type.value,
            marketplace=MarketplaceName.BLUEMOVE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        current_bid = CurrentNFTMarketplaceBid(
            token_data_id=bid_data.token_data_id,
            creator_address=bid_data.creator_address,
            token_name=bid_data.token_name,
            collection=bid_data.collection,
            collection_id=bid_data.collection_id,
            price=bid_data.price,
            token_amount=amount,
            buyer=bid_data.buyer,
            is_deleted=is_deleted,
            marketplace=standard_marketplace_event_type.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction_metadata.transaction_version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        parsed_objs.append(bid)
        parsed_objs.append(current_bid)

    # # Handle collection bid place and fill events
    # elif table_handle == BLUEMOVE_COLLECTION_BIDS_TABLE_HANDLE:
    #     collection_bid_metadata = parse_collection_bid(write_table_item)

    #     if collection_bid_metadata.seller:
    #         standard_marketplace_event_type = StandardMarketplaceEventType.BID_FILLED
    #         token_amount_change = -1
    #         is_current_bid_deleted = collection_bid_metadata.amount == 0
    #     else:
    #         standard_marketplace_event_type = StandardMarketplaceEventType.BID_PLACE
    #         token_amount_change = collection_bid_metadata.amount
    #         is_current_bid_deleted = False

    #     collection_bid = NFTMarketplaceCollectionBid(
    #         transaction_version=transaction_metadata.transaction_version,
    #         index=wsc_index,
    #         creator_address=collection_bid_metadata.creator_address,
    #         collection=collection_bid_metadata.collection,
    #         collection_id=collection_bid_metadata.collection_id,
    #         price=collection_bid_metadata.price,
    #         token_amount=token_amount_change,
    #         buyer=collection_bid_metadata.buyer,
    #         seller=collection_bid_metadata.seller,
    #         event_type=standard_marketplace_event_type.value,
    #         marketplace=MarketplaceName.BLUEMOVE.value,
    #         contract_address=transaction_metadata.contract_address,
    #         entry_function_id_str=transaction_metadata.entry_function_id_str_short,
    #         transaction_timestamp=transaction_metadata.transaction_timestamp,
    #     )
    #     current_collection_bid = CurrentNFTMarketplaceCollectionBid(
    #         creator_address=collection_bid_metadata.creator_address,
    #         collection=collection_bid_metadata.collection,
    #         collection_id=collection_bid_metadata.collection_id,
    #         price=collection_bid_metadata.price,
    #         token_amount=collection_bid_metadata.amount,
    #         buyer=collection_bid_metadata.buyer,
    #         is_deleted=is_current_bid_deleted,
    #         marketplace=MarketplaceName.BLUEMOVE.value,
    #         contract_address=transaction_metadata.contract_address,
    #         entry_function_id_str=transaction_metadata.entry_function_id_str_short,
    #         last_transaction_version=transaction_metadata.transaction_version,
    #         last_transaction_timestamp=transaction_metadata.transaction_timestamp,
    #     )

    #     parsed_objs.append(collection_bid)
    #     parsed_objs.append(current_collection_bid)

    return parsed_objs


def parse_listing(
    write_table_item: transaction_pb2.WriteTableItem,
) -> ListingTableMetadata | None:
    table_data = json.loads(write_table_item.data.value)

    # Price parsing
    price = table_data.get("price", None)
    price = int(price) if price else None

    # Seller parsing
    seller = table_data.get("seller", None)

    locked_token = table_data.get("locked_token", {}).get("vec", [])

    if not locked_token:
        return None

    token = locked_token[0]

    # Collection, token, and creator parsing
    token_data_id_struct = token.get("id", {}).get("token_data_id", {})
    collection = token_data_id_struct.get("collection", None)
    token_name = token_data_id_struct.get("name", None)
    creator = token_data_id_struct.get("creator", None)
    token_data_id_type = TokenDataIdType(creator, collection, token_name)

    # Amount parsing
    token_amount = token.get("amount", None)

    return ListingTableMetadata(
        price=price,
        seller=seller,
        creator_address=token_data_id_type.get_creator(),
        collection=token_data_id_type.get_collection_trunc(),
        collection_id=token_data_id_type.get_collection_data_id_hash(),
        token_name=token_data_id_type.get_name_trunc(),
        token_data_id=token_data_id_type.to_hash(),
        amount=token_amount,
    )


def parse_bid(write_table_item: transaction_pb2.WriteTableItem) -> BidMetadata | None:
    data = json.loads(write_table_item.data.value)

    # Parse seller
    seller = data.get("accept_address", None)
    if seller == "0x0":
        seller = None

    # Parse price, buyer, and seller
    offerer_struct = data.get("offerer", {})
    price = offerer_struct.get("amount", None)
    buyer = offerer_struct.get("offer_address", None)

    token_vec = data.get("token_id", {}).get("vec", [])
    if not token_vec:
        return None

    # Collection, token, and creator parsing
    token_data_id_struct = token_vec[0].get("token_data_id", {})
    collection = token_data_id_struct.get("collection", None)
    token_name = token_data_id_struct.get("name", None)
    creator = token_data_id_struct.get("creator", None)
    token_data_id_type = TokenDataIdType(creator, collection, token_name)

    return BidMetadata(
        price=price,
        amount=1,
        buyer=standardize_address(buyer) if buyer else None,
        seller=standardize_address(seller) if seller else None,
        creator_address=token_data_id_type.get_creator(),
        collection=token_data_id_type.get_collection_trunc(),
        collection_id=token_data_id_type.get_collection_data_id_hash(),
        token_name=token_data_id_type.get_name_trunc(),
        token_data_id=token_data_id_type.to_hash(),
    )


def parse_collection_bid(
    write_table_item: transaction_pb2.WriteTableItem,
) -> CollectionBidMetadata:
    data = json.loads(write_table_item.data.value)

    # Price and amount parsing
    price = data.get("amount_per_item", None)
    price = int(price) if price else None
    amount = data.get("quantity_offer_items", None)
    amount = int(amount) if amount else None

    # Collection, token, and creator parsing
    collection = data.get("collection_name", None)
    creator = data.get("creator_address", None)
    collection_data_id_type = CollectionDataIdType(creator, collection)

    # Buyer and seller parsing
    buyer = data.get("offerer", None)
    can_claim_tokens = data.get("can_claim_tokens", {}).get("data", [])
    seller = can_claim_tokens[0].get("value", None) if can_claim_tokens else None

    return CollectionBidMetadata(
        price=price,
        amount=amount,
        buyer=standardize_address(buyer) if buyer else None,
        seller=standardize_address(seller) if seller else None,
        collection=collection_data_id_type.get_name_trunc(),
        collection_id=collection_data_id_type.to_hash(),
        creator_address=collection_data_id_type.get_creator(),
        is_cancelled=None,
    )


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
