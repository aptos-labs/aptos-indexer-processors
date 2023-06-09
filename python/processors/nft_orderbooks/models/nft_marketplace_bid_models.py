from utils.models.annotated_types import (
    StringType,
    StringPrimaryKeyType,
    BigIntegerType,
    BigIntegerPrimaryKeyType,
    BooleanType,
    InsertedAtType,
    NumericType,
    NumericPrimaryKeyType,
    TimestampType,
)
from utils.models.general_models import Base


class CurrentNFTMarketplaceBid(Base):
    __tablename__ = "current_nft_marketplace_bids"

    token_data_id: StringPrimaryKeyType
    buyer: StringPrimaryKeyType
    price: NumericPrimaryKeyType
    creator_address: StringType
    amount: NumericType
    token_name: StringType
    collection: StringType
    collection_id: StringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    is_deleted: BooleanType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


class NFTMarketplaceBid(Base):
    __tablename__ = "nft_marketplace_bids"

    transaction_version: BigIntegerPrimaryKeyType
    index: BigIntegerPrimaryKeyType
    creator_address: StringType
    token_name: StringType
    token_data_id: StringType
    collection: StringType
    collection_id: StringType
    price: NumericType
    amount: NumericType
    buyer: StringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


class CurrentNFTMarketplaceCollectionBid(Base):
    __tablename__ = "current_nft_marketplace_collection_bids"

    collection_id: StringPrimaryKeyType
    buyer: StringPrimaryKeyType
    price: NumericPrimaryKeyType
    creator_address: StringType
    amount: NumericType
    collection: StringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    is_deleted: BooleanType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


class NFTMarketplaceCollectionBid(Base):
    __tablename__ = "nft_marketplace_collection_bids"

    transaction_version: BigIntegerPrimaryKeyType
    index: BigIntegerPrimaryKeyType
    creator_address: StringType
    collection: StringType
    collection_id: StringType
    price: NumericType
    amount: NumericType
    buyer: StringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType
