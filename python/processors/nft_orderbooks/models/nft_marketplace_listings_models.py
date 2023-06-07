from sqlalchemy.orm import Mapped
from utils.models.annotated_types import (
    StringType,
    BigIntegerType,
    BigIntegerPrimaryKeyType,
    InsertedAtType,
    JsonType,
    NullableNumericType,
    NullableStringType,
    NumericType,
    StringPrimaryKeyType,
    TimestampType,
)
from utils.models.general_models import Base


class CurrentNFTMartketplaceListing(Base):
    __tablename__ = "current_nft_marketplace_listings"

    token_data_id: StringPrimaryKeyType
    creator_address: StringType
    token_name: StringType 
    collection: StringType
    collection_id: StringType
    price: NumericType
    amount: NumericType
    seller: StringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


class NFTMarketplaceListing(Base):
    __tablename__ = "nft_marketplace_listings"

    transaction_version: BigIntegerPrimaryKeyType
    write_set_change_index: BigIntegerPrimaryKeyType
    creator_address: StringType
    token_name: StringType
    token_data_id: StringType
    collection: StringType
    collection_id: StringType
    price: NumericType
    amount: NumericType
    seller: StringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType