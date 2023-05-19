from typing import List
from aptos.transaction.testing1.v1 import transaction_pb2
from processors.nft_orderbooks.parsers import (
    bluemove_parser,
    itsrare_parser,
    okx_parser,
    ozozoz_parser,
    souffle_parser,
    topaz_parser,
)
from processors.nft_orderbooks.models.proto_autogen import (
    nft_marketplace_activities_pb2,
)

INDEXER_NAME = "nft_orderbooks"


def parse(
    transaction: transaction_pb2.Transaction,
) -> List[nft_marketplace_activities_pb2.NFTMarketplaceActivityRow]:
    nftActivities: List[nft_marketplace_activities_pb2.NFTMarketplaceActivityRow] = []
    nftActivities.extend(bluemove_parser.parse_marketplace_events(transaction))
    nftActivities.extend(itsrare_parser.parse_marketplace_events(transaction))
    nftActivities.extend(okx_parser.parse_marketplace_events(transaction))
    nftActivities.extend(ozozoz_parser.parse_marketplace_events(transaction))
    nftActivities.extend(souffle_parser.parse_marketplace_events(transaction))
    nftActivities.extend(topaz_parser.parse_marketplace_events(transaction))
    return nftActivities
