from typing import List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName
from processors.nft_orderbooks.nft_marketplace_constants import (
    MARKETPLACE_ENTRY_FUNCTIONS,
    MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV,
    MARKETPLACE_TABLE_HANDLES_INV,
)
from processors.nft_marketplace_v2 import marketplace_v2_parser
from processors.nft_marketplace_v2.constants import MARKETPLACE_V2_ADDRESS
from processors.nft_marketplace_v2.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceActivities,
)
from utils.transactions_processor import TransactionsProcessor
from utils import event_utils, general_utils, transaction_utils, write_set_change_utils
from utils.token_utils import TokenV2AggregatedDataMapping


def parse(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceActivities | CurrentNFTMarketplaceListing]:
    parsed_objs = []
    user_transaction = transaction_utils.get_user_transaction(transaction)

    if not user_transaction:
        return []

    # Parse activities
    parsed_objs.extend(marketplace_v2_parser.parse_activities(transaction))
    # Parse listing
    parsed_objs.extend(marketplace_v2_parser.parse_listing(transaction))

    # Parse write set changes for bids

    # Parse write set changes for token and collection offers

    return parsed_objs


if __name__ == "__main__":
    transactions_processor = TransactionsProcessor(
        parser_function=parse,
        processor_name="nft-marketplace-v2",
    )
    transactions_processor.process()
