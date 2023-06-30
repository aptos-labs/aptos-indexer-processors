import json
from typing import List
from aptos.transaction.v1 import transaction_pb2
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
                creator_address=token_data_id_type.get_creator(),
                collection_id=token_data_id_type.get_collection_data_id_hash(),
                collection_name=token_data_id_type.get_collection_trunc(),
                token_name=token_data_id_type.get_name_trunc(),
                token_data_id_v1=token_data_id_type.to_hash(),
            )

    return None


def get_token_metadata_v2(
    write_set_change: transaction_pb2.WriteSetChange,
) -> TokenV2AggregatedData | None:
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
                    listing_address = write_resource.address
                    index = wsc_index
                    seller = data.get("seller", None)
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
                token_standard = TokenStandard.TOKEN_V2
                token_data_id = resource_address

    if listing_address and token_data_id:
        token_metadata = token_metadata_mapping[token_data_id]

        current_listing = CurrentNFTMarketplaceListing(
            token_data_id=token_data_id,
            listing_address=listing_address,
            creator_address=token_metadata.creator_address,
            token_name=token_metadata.token_name,
            collection=token_metadata.collection_name,
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
            creator_address=token_metadata.creator_address,
            token_name=token_metadata.token_name,
            token_data_id=token_data_id,
            collection=token_metadata.collection_name,
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
