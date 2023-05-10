from typing import List
from aptos.transaction.testing1.v1 import transaction_pb2
from processors.nft_orderbooks.topaz_parser import parse_topaz_marketplace_events
from models.proto_autogen import nft_marketplace_activities_pb2

INDEXER_NAME = "nft_orderbooks"


def parse(
    transaction: transaction_pb2.Transaction,
) -> List[nft_marketplace_activities_pb2.NFTMarketplaceActivityRow]:
    nftActivities: List[nft_marketplace_activities_pb2.NFTMarketplaceActivityRow] = []
    nftActivities.extend(parse_topaz_marketplace_events(transaction)) 

    if len(nftActivities) > 0:
        print(nftActivities)

    return nftActivities
