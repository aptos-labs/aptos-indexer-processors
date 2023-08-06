from typing import List
from aptos.transaction.v1 import transaction_pb2
from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName
from processors.nft_orderbooks.nft_marketplace_constants import (
    MARKETPLACE_ENTRY_FUNCTIONS,
    MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV,
    MARKETPLACE_TABLE_HANDLES_INV,
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
    NFTMarketplaceCollectionBid,
    CurrentNFTMarketplaceCollectionBid,
)
from processors.nft_orderbooks.models.nft_marketplace_listings_models import (
    CurrentNFTMarketplaceListing,
    NFTMarketplaceListing,
)
from processors.nft_orderbooks.nft_orderbooks_parser_utils import MarketplaceName
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from utils.transactions_processor import TransactionsProcessor, ProcessingResult
from utils import event_utils, general_utils, transaction_utils, write_set_change_utils
from utils.models.schema_names import NFT_MARKETPLACE_SCHEMA_NAME
from utils.session import Session
from utils.processor_name import ProcessorName


class NFTMarketplaceProcesser(TransactionsProcessor):
    def name(self) -> str:
        return ProcessorName.NFT_MARKETPLACE_V1_PROCESSOR.value

    def schema(self) -> str:
        return NFT_MARKETPLACE_SCHEMA_NAME

    def process_transactions(
        self,
        transactions: list[transaction_pb2.Transaction],
        start_version: int,
        end_version: int,
    ) -> ProcessingResult:
        parsed_objs = []

        for transaction in transactions:
            user_transaction = transaction_utils.get_user_transaction(transaction)

            # Filter user transactions
            if not user_transaction:
                continue

            events = user_transaction.events
            for event_index, event in enumerate(events):
                contract_address = general_utils.standardize_address(
                    event_utils.get_event_type_address(event)
                )

                if contract_address not in MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV:
                    continue

                marketplace_name = MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV[
                    contract_address
                ]

                # TODO: Optimize this; there's too many loops
                match (marketplace_name):
                    case MarketplaceName.TOPAZ:
                        parsed_objs.extend(
                            topaz_parser.parse_event(transaction, event, event_index)
                        )
                    case MarketplaceName.SOUFFLE:
                        parsed_objs.extend(
                            souffle_parser.parse_event(transaction, event, event_index)
                        )
                    case MarketplaceName.BLUEMOVE:
                        parsed_objs.extend(
                            bluemove_parser.parse_event(transaction, event, event_index)
                        )
                    case MarketplaceName.OKX:
                        parsed_objs.extend(
                            okx_parser.parse_marketplace_events(transaction)
                        )
                    case MarketplaceName.OZOZOZ:
                        parsed_objs.extend(
                            ozozoz_parser.parse_marketplace_events(transaction)
                        )
                    case MarketplaceName.ITSRARE:
                        parsed_objs.extend(
                            itsrare_parser.parse_marketplace_events(transaction)
                        )

            write_set_changes = transaction_utils.get_write_set_changes(transaction)
            for wsc_index, wsc in enumerate(write_set_changes):
                write_table_item = write_set_change_utils.get_write_table_item(wsc)

                if write_table_item:
                    table_handle = str(write_table_item.handle)

                    if table_handle not in MARKETPLACE_TABLE_HANDLES_INV:
                        continue

                    marketplace_name = MARKETPLACE_TABLE_HANDLES_INV[table_handle]

                    match (marketplace_name):
                        case MarketplaceName.TOPAZ:
                            parsed_objs.extend(
                                topaz_parser.parse_write_table_item(
                                    transaction, write_table_item, wsc_index
                                )
                            )
                        case MarketplaceName.SOUFFLE:
                            parsed_objs.extend(
                                souffle_parser.parse_write_table_item(
                                    transaction, write_table_item, wsc_index
                                )
                            )
                        case MarketplaceName.BLUEMOVE:
                            parsed_objs.extend(
                                bluemove_parser.parse_write_table_item(
                                    transaction, write_table_item, wsc_index
                                )
                            )

        # TODO: Sort by pk for multi threaded postgres insert

        self.insert_to_db(
            parsed_objs,
        )

        return ProcessingResult(
            start_version=start_version,
            end_version=end_version,
        )

    def insert_to_db(
        self,
        parsed_objs: List[
            NFTMarketplaceEvent
            | NFTMarketplaceListing
            | CurrentNFTMarketplaceListing
            | NFTMarketplaceBid
            | CurrentNFTMarketplaceBid
            | NFTMarketplaceCollectionBid
            | CurrentNFTMarketplaceCollectionBid
        ],
    ) -> None:
        with Session() as session, session.begin():
            # TODO: Turn this into on conflict, update, to support backfilling
            for obj in parsed_objs:
                session.merge(obj)
