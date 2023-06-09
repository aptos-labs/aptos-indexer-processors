from typing import List, Tuple
from aptos.transaction.v1 import transaction_pb2
from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName
from processors.nft_orderbooks.nft_marketplace_constants import (
    MARKETPLACE_ENTRY_FUNCTIONS,
    MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV,
)
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
from processors.nft_orderbooks.models.nft_marketplace_bid_models import (
    NFTMarketplaceBid,
    CurrentNFTMarketplaceBid,
)
from processors.nft_orderbooks.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import MarketplaceName
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from utils.models.general_models import NextVersionToProcess
from utils.transactions_processor import TransactionsProcessor
from utils import transaction_utils


def parse(
    transaction: transaction_pb2.Transaction,
) -> List[
    NFTMarketplaceEvent
    | NFTMarketplaceListing
    | CurrentNFTMarketplaceListing
    | NFTMarketplaceBid
    | CurrentNFTMarketplaceBid
]:
    nft_activities: List[NFTMarketplaceEvent] = []
    nft_marketplace_listings: List[NFTMarketplaceListing] = []
    current_nft_marketplace_listings: List[CurrentNFTMarketplaceListing] = []
    nft_marketplace_bids: List[NFTMarketplaceBid] = []
    current_nft_marketplace_bids: List[CurrentNFTMarketplaceBid] = []

    user_transaction = transaction_utils.get_user_transaction(transaction)

    # Filter user transactions
    if not user_transaction:
        return []

    contract_address = (
        transaction_utils.get_contract_address(user_transaction)
        if user_transaction
        else None
    )

    # Filter marketplace smart contracts
    if contract_address not in MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV:
        return []

    marketplace_name = MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV[contract_address]

    # Filter marketplace entry functions
    entry_function_id_str_short = transaction_utils.get_entry_function_id_str_short(
        user_transaction
    )
    if entry_function_id_str_short not in MARKETPLACE_ENTRY_FUNCTIONS[marketplace_name]:
        return []

    match (marketplace_name):
        case MarketplaceName.TOPAZ:
            (
                nft_activities,
                nft_marketplace_listings,
                current_nft_marketplace_listings,
                nft_marketplace_bids,
                current_nft_marketplace_bids,
            ) = topaz_parser.parse_transaction(transaction)
        case MarketplaceName.SOUFFLE:
            nft_activities.extend(souffle_parser.parse_marketplace_events(transaction))
        case MarketplaceName.BLUEMOVE:
            nft_activities.extend(bluemove_parser.parse_marketplace_events(transaction))
        case MarketplaceName.OKX:
            nft_activities.extend(okx_parser.parse_marketplace_events(transaction))
        case MarketplaceName.OZOZOZ:
            nft_activities.extend(ozozoz_parser.parse_marketplace_events(transaction))
        case MarketplaceName.ITSRARE:
            nft_activities.extend(itsrare_parser.parse_marketplace_events(transaction))

    return (
        nft_activities
        + nft_marketplace_listings
        + current_nft_marketplace_listings
        + nft_marketplace_bids
        + current_nft_marketplace_bids
    )


# class NFTMarketplaceTransactionsProcessor(TransactionsProcessor):
#     def insert_to_db(
#         self,
#         parsed_objs: Tuple[
#             NFTMarketplaceEvent, NFTMarketplaceListing, CurrentNFTMarketplaceListing
#         ],
#         txn_version: int,
#     ) -> None:
#         indexer_name = self.config.indexer_name
#         (
#             nft_marketplace_activities,
#             nft_marketplace_listings,
#             current_nft_marketplace_listings,
#         ) = parsed_objs

#         # If we find relevant transactions add them and update latest processed version
#         if parsed_objs is not None:
#             with Session(self.engine) as session, session.begin():
#                 # Simple merge
#                 simple_merge_objs = (
#                     nft_marketplace_activities + nft_marketplace_listings + current_nft_marketplace_listings
#                 )
#                 for obj in simple_merge_objs:
#                     session.merge(obj)

#                 # # Custom inserts
#                 # self.insert_current_listings_to_db(
#                 #     current_nft_marketplace_listings, session
#                 # )

#                 # Update latest processed version
#                 session.merge(
#                     NextVersionToProcess(
#                         indexer_name=indexer_name,
#                         next_version=txn_version + 1,
#                     )
#                 )
#         # If we don't find any relevant transactions, at least update latest processed version every 1000
#         elif (txn_version % 1000) == 0:
#             with Session(self.engine) as session, session.begin():
#                 # Update latest processed version
#                 session.merge(
#                     NextVersionToProcess(
#                         indexer_name=indexer_name,
#                         next_version=txn_version + 1,
#                     )
#                 )

#     def insert_current_listings_to_db(
#         self, current_listings: List[CurrentNFTMarketplaceListing], session: Session
#     ) -> None:
#         for current_listing in current_listings:
#             session.merge(current_listing)


if __name__ == "__main__":
    transactions_processor = TransactionsProcessor(
        parse,
    )
    transactions_processor.process()
