from typing import List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName
from processors.nft_orderbooks.nft_marketplace_constants import (
    MARKETPLACE_ENTRY_FUNCTIONS,
    MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV,
    MARKETPLACE_TABLE_HANDLES_INV,
)
from processors.nft_marketplace_v2 import marketplace_v2_parser
from processors.nft_marketplace_v2.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from utils.transactions_processor import TransactionsProcessor
from utils import event_utils, general_utils, transaction_utils, write_set_change_utils
from utils.token_utils import TokenV2AggregatedDataMapping


MARKETPLACE_V2_ADDRESS = (
    "0xb11affd5c514bb969e988710ef57813d9556cc1e3fe6dc9aa6a82b56aee53d98"
)


def parse(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceListing | CurrentNFTMarketplaceListing]:
    parsed_objs = []
    user_transaction = transaction_utils.get_user_transaction(transaction)

    if not user_transaction:
        return parsed_objs

    write_set_changes = transaction_utils.get_write_set_changes(transaction)

    # Get token metadata
    token_mapping: TokenV2AggregatedDataMapping = {}
    for _, wsc in enumerate(write_set_changes):
        token_metadata_v1 = marketplace_v2_parser.get_token_metadata_v1(wsc)
        if token_metadata_v1:
            token_key = token_metadata_v1.token_data_id_v1
            assert token_key
            token_mapping[token_key] = token_metadata_v1
            print("found token v1", token_key)

        token_metadata_v2 = marketplace_v2_parser.get_token_metadata_v2(wsc)
        if token_metadata_v2:
            token_key = token_metadata_v2.token_address_v2
            assert token_key
            token_mapping[token_key] = token_metadata_v2
            print("found token v2", token_key)

    # Parse write set changes for listings
    for wsc_index, wsc in enumerate(write_set_changes):
        write_resource = write_set_change_utils.get_write_resource(wsc)

        if write_resource:
            address = write_resource.type.address
            if address != MARKETPLACE_V2_ADDRESS:
                continue

            resource_type_short = write_set_change_utils.get_move_type_short(
                write_resource.type
            )
            match resource_type_short:
                case "coin_listing::FixedPriceListing<0x1::aptos_coin::AptosCoin>":
                    marketplace_v2_parser.parse_fixed_price_listing()
                case "listing::TokenV1Container":
                    marketplace_v2_parser.parse_listing_token_v1_container()
                case "listing::Listing":
                    marketplace_v2_parser.parse_listing()

    # Parse write set changes for bids

    # Parse write set changes for token and collection offers

    return parsed_objs


if __name__ == "__main__":
    transactions_processor = TransactionsProcessor(
        parser_function=parse,
        processor_name="nft-marketplace-v2",
    )
    transactions_processor.process()
