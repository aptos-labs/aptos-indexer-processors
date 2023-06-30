import json
from typing import List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_marketplace_v2.marketplace_v2_utils import (
    lookup_v2_current_listing_in_db,
)
from processors.nft_marketplace_v2.constants import MARKETPLACE_V2_ADDRESS
from processors.nft_marketplace_v2.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    parse_transaction_metadata,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    StandardMarketplaceEventType,
    MarketplaceName,
)
from utils import event_utils, transaction_utils, write_set_change_utils
from utils.token_utils import (
    TokenStandard,
    TokenDataIdType,
    TokenV2AggregatedData,
    TokenV2AggregatedDataMapping,
)
from utils.general_utils import standardize_address


def get_token_metadata_v1(
    write_set_change: transaction_pb2.WriteSetChange,
) -> TokenV2AggregatedData | None:
    if write_set_change.type not in set(
        [
            transaction_pb2.WriteSetChange.TYPE_WRITE_TABLE_ITEM,
            transaction_pb2.WriteSetChange.TYPE_DELETE_TABLE_ITEM,
        ]
    ):
        return None

    table_item = (
        write_set_change.write_table_item
        if write_set_change.type == transaction_pb2.WriteSetChange.TYPE_WRITE_TABLE_ITEM
        else write_set_change.delete_table_item
    )
    data = table_item.data

    if data.key_type == "0x3::token::TokenId":
        key = json.loads(data.key)
        token_data_id_struct = key.get("token_data_id")
        if token_data_id_struct:
            collection = token_data_id_struct.get("collection", None)
            creator = token_data_id_struct.get("creator", None)
            name = token_data_id_struct.get("name", None)

            token_data_id_type = TokenDataIdType(creator, collection, name)
            return TokenV2AggregatedData(
                token_data_id=token_data_id_type.to_hash(),
                collection_id=token_data_id_type.get_collection_data_id_hash(),
                token_name=token_data_id_type.get_name_trunc(),
                token_standard=TokenStandard.TOKEN_V1,
            )

    return None


def get_token_metadata_v2(
    write_set_change: transaction_pb2.WriteSetChange,
) -> TokenV2AggregatedData | None:
    if write_set_change.type != transaction_pb2.WriteSetChange.TYPE_WRITE_RESOURCE:
        return None

    write_resource = write_set_change.write_resource

    if write_resource.type_str == "0x4::token::Token":
        data = json.loads(write_resource.data)

        token_address = write_resource.address
        collection_id = data.get("collection", {}).get("inner", None)
        name = data.get("name", None)

        return TokenV2AggregatedData(
            token_data_id=token_address,
            collection_id=collection_id,
            token_name=name,
            token_standard=TokenStandard.TOKEN_V2,
        )

    return None


def parse_listing(
    transaction: transaction_pb2.Transaction,
    token_metadata_mapping: TokenV2AggregatedDataMapping,
) -> List[NFTMarketplaceListing | CurrentNFTMarketplaceListing]:
    parsed_objs = []

    user_transaction = transaction_utils.get_user_transaction(transaction)
    assert user_transaction

    transaction_metadata = parse_transaction_metadata(transaction)
    events = user_transaction.events
    write_set_changes = transaction_utils.get_write_set_changes(transaction)

    # Parse listing deletes and fills
    for event_index, event in enumerate(events):
        qualified_event_type = event.type_str

        if MARKETPLACE_V2_ADDRESS not in qualified_event_type:
            continue

        event_type_short = event_utils.get_event_type_short(event)
        if event_type_short == "coin_listing::PurchaseEvent":
            data = json.loads(event.data)
            price = data.get("price", None)

            if not price:
                continue

            listing_address = standardize_address(event.key.account_address)
            current_listing_from_db = lookup_v2_current_listing_in_db(listing_address)

            if current_listing_from_db:
                seller = None
                buyer = None
                price = int(price)

                if price == 0:
                    standard_event_type = StandardMarketplaceEventType.LISTING_CANCEL
                    seller = data.get("purchaser", None)
                    seller = standardize_address(seller) if seller else None
                else:
                    standard_event_type = StandardMarketplaceEventType.LISTING_FILLED
                    buyer = data.get("purchaser", None)
                    buyer = standardize_address(buyer) if buyer else None
                    seller = current_listing_from_db.seller

                current_listing = CurrentNFTMarketplaceListing(
                    token_data_id=current_listing_from_db.token_data_id,
                    listing_address=listing_address,
                    token_name=current_listing_from_db.token_name,
                    collection_id=current_listing_from_db.collection_id,
                    price=0,
                    token_amount=1,
                    token_standard=current_listing_from_db.token_standard,
                    seller=seller,
                    is_deleted=True,
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    last_transaction_version=transaction.version,
                    last_transaction_timestamp=transaction_metadata.transaction_timestamp,
                )
                listing = NFTMarketplaceListing(
                    transaction_version=transaction.version,
                    index=event_index,
                    listing_address=listing_address,
                    token_name=current_listing_from_db.token_name,
                    token_data_id=current_listing_from_db.token_data_id,
                    collection_id=current_listing_from_db.collection_id,
                    price=current_listing_from_db.price,
                    token_amount=-1,
                    token_standard=current_listing_from_db.token_standard,
                    seller=seller,
                    buyer=buyer,
                    marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
                    contract_address=transaction_metadata.contract_address,
                    entry_function_id_str=transaction_metadata.entry_function_id_str_short,
                    event_type=standard_event_type.value,
                    transaction_timestamp=transaction_metadata.transaction_timestamp,
                )

                parsed_objs.extend([current_listing, listing])

    listing_address = None
    inner = None
    token_data_id = None  # Token address for v2, token data id for v1
    price = None
    seller = None
    token_standard = TokenStandard.TOKEN_V2
    index = None

    # Parse listing place
    for wsc_index, wsc in enumerate(write_set_changes):
        write_resource = write_set_change_utils.get_write_resource(wsc)
        if write_resource:
            move_type_address = write_resource.type.address
            if move_type_address != MARKETPLACE_V2_ADDRESS:
                continue

            move_type_short = write_set_change_utils.get_move_type_short(
                write_resource.type
            )
            data = json.loads(write_resource.data)

            match move_type_short:
                case "coin_listing::FixedPriceListing":
                    price = data.get("price", None)
                case "listing::Listing":
                    listing_address = standardize_address(write_resource.address)
                    index = wsc_index
                    seller = data.get("seller", None)
                    seller = standardize_address(seller) if seller else None
                    inner = data.get("object", {}).get("inner", None)

    # Parse token v1 container or v2 token
    for _, wsc in enumerate(write_set_changes):
        write_resource = write_set_change_utils.get_write_resource(wsc)
        if write_resource:
            resource_address = write_resource.address
            if resource_address != inner:
                continue
            data = json.loads(write_resource.data)
            move_type = write_resource.type_str

            if move_type == f"{MARKETPLACE_V2_ADDRESS}::listing::TokenV1Container":
                token_standard = TokenStandard.TOKEN_V1

                token_data_id_struct = (
                    data.get("token", {}).get("id", {}).get("token_data_id", {})
                )
                collection = token_data_id_struct.get("collection", None)
                creator = token_data_id_struct.get("creator", None)
                name = token_data_id_struct.get("name", None)

                token_data_id_type = TokenDataIdType(creator, collection, name)
                token_data_id = token_data_id_type.to_hash()
            elif move_type == "0x4::token::Token":
                token_data_id = resource_address

    if listing_address and token_data_id:
        token_metadata = token_metadata_mapping[token_data_id]

        current_listing = CurrentNFTMarketplaceListing(
            token_data_id=token_data_id,
            listing_address=listing_address,
            token_name=token_metadata.token_name,
            collection_id=token_metadata.collection_id,
            price=price,
            token_amount=1,
            token_standard=token_standard.value,
            seller=seller,
            is_deleted=False,
            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            last_transaction_version=transaction.version,
            last_transaction_timestamp=transaction_metadata.transaction_timestamp,
        )
        listing = NFTMarketplaceListing(
            transaction_version=transaction.version,
            index=index,
            listing_address=listing_address,
            token_name=token_metadata.token_name,
            token_data_id=token_data_id,
            collection_id=token_metadata.collection_id,
            price=price,
            token_amount=1,
            token_standard=token_standard.value,
            seller=seller,
            buyer=None,
            marketplace=MarketplaceName.EXAMPLE_V2_MARKETPLACE.value,
            contract_address=transaction_metadata.contract_address,
            entry_function_id_str=transaction_metadata.entry_function_id_str_short,
            event_type=StandardMarketplaceEventType.LISTING_PLACE.value,
            transaction_timestamp=transaction_metadata.transaction_timestamp,
        )

        parsed_objs.extend([current_listing, listing])

    return parsed_objs
