from utils.models.annotated_types import (
    NullableStringType,
    StringType,
    BigIntegerType,
    BigIntegerPrimaryKeyType,
    BooleanType,
    InsertedAtType,
    NumericType,
    StringPrimaryKeyType,
    TimestampType,
    NullableNumericType,
    UpdatedAtType,
)
from utils.models.general_models import Base
from utils.models.schema_names import NFT_MARKETPLACE_V2_SCHEMA_NAME
from sqlalchemy import Index


class NFTMarketplaceActivities(Base):
    __tablename__ = "nft_marketplace_activities"
    __table_args__ = (
        (Index("ev_offer_or_listing_index", "offer_or_listing_id")),
        (Index("ev_token_data_id_index", "token_data_id")),
        (Index("ev_collection_id_index", "collection_id")),
        (Index("ev_fee_schedule_index", "fee_schedule_id")),
        {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME},
    )

    transaction_version: BigIntegerPrimaryKeyType
    event_index: BigIntegerPrimaryKeyType
    offer_or_listing_id: StringType
    fee_schedule_id: StringType
    collection_id: StringType
    token_data_id: NullableStringType
    creator_address: StringType
    collection_name: StringType
    token_name: NullableStringType
    property_version: NullableStringType
    price: NumericType
    token_amount: NumericType
    token_standard: StringType
    seller: NullableStringType
    buyer: NullableStringType
    coin_type: NullableStringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    event_type: StringType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


class CurrentNFTMarketplaceListing(Base):
    __tablename__ = "current_nft_marketplace_listings"
    __table_args__ = (
        (Index("curr_list_collection_index", "collection_id")),
        (Index("curr_list_fee_schedule_index", "fee_schedule_id")),
        (Index("curr_list_collection_price_index", "collection_id", "price")),
        (Index("curr_list_seller_index", "seller")),
        {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME},
    )

    listing_id: StringPrimaryKeyType
    token_data_id: StringPrimaryKeyType
    collection_id: StringType
    fee_schedule_id: StringType
    price: NumericType
    token_amount: NumericType
    token_standard: StringType
    seller: StringType
    is_deleted: BooleanType
    coin_type: NullableStringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


# class NFTMarketplaceListing(Base):
#     __tablename__ = "nft_marketplace_listings"
#     __table_args__ = {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME}

#     transaction_version: BigIntegerPrimaryKeyType
#     write_set_index: BigIntegerPrimaryKeyType
#     token_data_id: StringPrimaryKeyType
#     listing_address: NullableStringType
#     price: NumericType
#     token_amount: NumericType
#     token_standard: StringType
#     seller: StringType
#     is_deleted: BooleanType
#     marketplace: StringType
#     contract_address: StringType
#     entry_function_id_str: StringType
#     transaction_timestamp: TimestampType
#     inserted_at: InsertedAtType


class CurrentNFTMarketplaceTokenOffer(Base):
    __tablename__ = "current_nft_marketplace_token_offers"
    __table_args__ = (
        (Index("curr_tok_offer_collection_index", "collection_id")),
        (Index("curr_tok_offer_fee_schedule_index", "fee_schedule_id")),
        (Index("curr_tok_offer_buyer_index", "buyer")),
        {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME},
    )

    offer_id: StringPrimaryKeyType
    token_data_id: StringPrimaryKeyType
    collection_id: StringType
    fee_schedule_id: StringType
    buyer: StringType
    price: NumericType
    token_amount: NumericType
    expiration_time: NumericType
    is_deleted: BooleanType
    token_standard: StringType
    coin_type: NullableStringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


# class NFTMarketplaceOffer(Base):
#     __tablename__ = "nft_marketplace_token_offers"
#     __table_args__ = {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME}

#     transaction_version: BigIntegerPrimaryKeyType
#     index: BigIntegerPrimaryKeyType
#     creator_address: StringType
#     token_name: StringType
#     token_data_id: StringType
#     collection: StringType
#     collection_id: StringType
#     price: NumericType
#     token_amount: NumericType
#     expirationTime: NumericType
#     buyer: StringType
#     seller: NullableStringType
#     marketplace: StringType
#     contract_address: StringType
#     entry_function_id_str: StringType
#     event_type: StringType
#     transaction_timestamp: TimestampType
#     inserted_at: InsertedAtType


class CurrentNFTMarketplaceCollectionOffer(Base):
    __tablename__ = "current_nft_marketplace_collection_offers"
    __table_args__ = (
        (Index("curr_coll_off_fee_schedule_index", "fee_schedule_id")),
        (Index("curr_coll_off_buyer_index", "buyer")),
        (Index("curr_coll_off_price_index", "item_price")),
        {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME},
    )

    collection_offer_id: StringPrimaryKeyType
    collection_id: StringPrimaryKeyType
    fee_schedule_id: StringType
    buyer: StringType
    item_price: NumericType
    remaining_token_amount: NumericType
    expiration_time: NumericType
    is_deleted: BooleanType
    token_standard: StringType
    coin_type: NullableStringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


# class NFTMarketplaceCollectionBid(Base):
#     __tablename__ = "nft_marketplace_collection_bids"
#     __table_args__ = {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME}

#     transaction_version: BigIntegerPrimaryKeyType
#     index: BigIntegerPrimaryKeyType
#     creator_address: StringType
#     collection: StringType
#     collection_id: StringType
#     price: NumericType
#     token_amount: NumericType
#     buyer: StringType
#     seller: NullableStringType
#     marketplace: StringType
#     contract_address: StringType
#     entry_function_id_str: StringType
#     event_type: StringType
#     transaction_timestamp: TimestampType
#     inserted_at: InsertedAtType


class CurrentNFTMarketplaceAuction(Base):
    __tablename__ = "current_nft_marketplace_auctions"
    __table_args__ = (
        (Index("curr_auc_collection_index", "collection_id")),
        (Index("curr_auc_fee_schedule_index", "fee_schedule_id")),
        (Index("curr_auc_seller_index", "seller")),
        (Index("curr_auc_bidder_index", "current_bidder")),
        {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME},
    )

    listing_id: StringPrimaryKeyType
    token_data_id: StringPrimaryKeyType
    collection_id: StringType
    fee_schedule_id: StringType
    seller: StringType
    current_bid_price: NullableNumericType
    current_bidder: NullableStringType
    starting_bid_price: NumericType
    buy_it_now_price: NullableNumericType
    token_amount: NumericType
    expiration_time: NumericType
    is_deleted: BooleanType
    token_standard: StringType
    coin_type: NullableStringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType
