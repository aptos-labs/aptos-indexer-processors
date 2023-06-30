import json
from typing import List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_marketplace_v2.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import (
    parse_transaction_metadata,
)
from utils import transaction_utils, write_set_change_utils
from utils.token_utils import TokenDataIdType, TokenV2AggregatedData


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
                collection_address=token_data_id_type.get_collection_data_id_hash(),
                collection_name=token_data_id_type.get_collection_trunc(),
                token_name=token_data_id_type.get_name_trunc(),
                token_data_id_v1=token_data_id_type.to_hash(),
            )

    return None


def get_token_metadata_v2(
    write_set_change: transaction_pb2.WriteSetChange,
) -> TokenV2AggregatedData | None:
    return None


def parse_fixed_price_listing():
    pass


def parse_listing():
    pass


def parse_listing_token_v1_container():
    pass
