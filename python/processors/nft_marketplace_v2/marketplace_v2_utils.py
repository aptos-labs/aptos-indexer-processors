import datetime
import re

from aptos.transaction.v1 import transaction_pb2
from dataclasses import dataclass
from typing import List
from processors.nft_orderbooks.nft_marketplace_constants import (
    MARKETPLACE_SMART_CONTRACT_ADDRESSES,
    MARKETPLACE_ENTRY_FUNCTIONS,
    MARKETPLACE_ADDRESS_MATCH_REGEX_STRINGS,
)
from processors.nft_orderbooks.nft_marketplace_enums import (
    MarketplaceName,
    TransactionMetadata,
    ListingTableMetadata,
    BidMetadata,
)
from processors.nft_marketplace_v2.nft_marketplace_models import (
    CurrentNFTMarketplaceListing,
)
from processors.nft_orderbooks.models.nft_marketplace_bid_models import (
    CurrentNFTMarketplaceBid,
    CurrentNFTMarketplaceCollectionBid,
)
from utils import general_utils, transaction_utils
from utils.session import Session


def lookup_v2_current_listing_in_db(
    listing_id: str,
) -> CurrentNFTMarketplaceListing | None:
    listing = None

    with Session() as session, session.begin():
        listing_from_db = (
            session.query(CurrentNFTMarketplaceListing)
            .filter(CurrentNFTMarketplaceListing.listing_id == listing_id)
            .one_or_none()
        )

        if listing_from_db:
            listing = CurrentNFTMarketplaceListing(
                token_data_id=listing_from_db.token_data_id,
                listing_id=listing_from_db.listing_id,
                price=listing_from_db.price,
                token_amount=listing_from_db.token_amount,
                token_standard=listing_from_db.token_standard,
                seller=listing_from_db.seller,
                is_deleted=listing_from_db.is_deleted,
                marketplace=listing_from_db.marketplace,
                contract_address=listing_from_db.contract_address,
                entry_function_id_str=listing_from_db.entry_function_id_str,
                last_transaction_version=listing_from_db.last_transaction_version,
                last_transaction_timestamp=listing_from_db.last_transaction_timestamp,
                inserted_at=listing_from_db.inserted_at,
            )

    return listing
