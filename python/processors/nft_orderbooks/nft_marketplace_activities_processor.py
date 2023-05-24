from typing import List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_orderbooks.parsers import (
    bluemove_parser,
    itsrare_parser,
    okx_parser,
    ozozoz_parser,
    souffle_parser,
    topaz_parser,
)
from processors.nft_orderbooks.models.nft_marketplace_activities_model import (
    NFTMarketplaceEvent,
)
from utils.transactions_processor import TransactionsProcessor


def parse(
    transaction: transaction_pb2.Transaction,
) -> List[NFTMarketplaceEvent]:
    nftActivities: List[NFTMarketplaceEvent] = []
    nftActivities.extend(bluemove_parser.parse_marketplace_events(transaction))
    nftActivities.extend(itsrare_parser.parse_marketplace_events(transaction))
    nftActivities.extend(okx_parser.parse_marketplace_events(transaction))
    nftActivities.extend(ozozoz_parser.parse_marketplace_events(transaction))
    nftActivities.extend(souffle_parser.parse_marketplace_events(transaction))
    nftActivities.extend(topaz_parser.parse_marketplace_events(transaction))
    return nftActivities


if __name__ == "__main__":
    transactions_processor = TransactionsProcessor(
        parse,
    )
    transactions_processor.process()
