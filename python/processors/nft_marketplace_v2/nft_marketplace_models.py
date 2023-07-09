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
)
from utils.models.general_models import Base
from utils.models.schema_names import NFT_MARKETPLACE_V2_SCHEMA_NAME


class NFTMarketplaceActivities(Base):
    __tablename__ = "nft_marketplace_activities"
    __table_args__ = {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME}

    transaction_version: BigIntegerPrimaryKeyType
    event_index: BigIntegerPrimaryKeyType
    offer_or_listing_id: StringType
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
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    event_type: StringType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


class CurrentNFTMarketplaceListing(Base):
    __tablename__ = "current_nft_marketplace_listings"
    __table_args__ = {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME}

    token_data_id: StringPrimaryKeyType
    listing_address: NullableStringType
    price: NumericType
    token_amount: NumericType
    token_standard: StringType
    seller: StringType
    is_deleted: BooleanType
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
    __table_args__ = {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME}

    offer_id: StringPrimaryKeyType
    token_data_id: StringType
    buyer: StringType
    price: NumericType
    token_amount: NumericType
    expiration_time: NumericType
    is_deleted: BooleanType
    token_standard: StringType
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
    __table_args__ = {"schema": NFT_MARKETPLACE_V2_SCHEMA_NAME}

    collection_offer_id: StringPrimaryKeyType
    collection_id: StringType
    buyer: StringType
    item_price: NumericType
    remaining_token_amount: NumericType
    expiration_time: NumericType
    is_deleted: BooleanType
    token_standard: StringType
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
