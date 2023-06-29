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
from utils.models.schema_names import NFT_MARKETPLACE_SCHEMA_NAME


class CurrentNFTMarketplaceListing(Base):
    __tablename__ = "current_nft_marketplace_listings"
    __table_args__ = {"schema": NFT_MARKETPLACE_SCHEMA_NAME}

    token_data_id: StringPrimaryKeyType
    creator_address: StringType
    token_name: StringType
    collection: StringType
    collection_id: StringType
    price: NumericType
    token_amount: NumericType
    seller: StringType
    is_deleted: BooleanType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    last_transaction_version: BigIntegerType
    last_transaction_timestamp: TimestampType
    inserted_at: InsertedAtType


class NFTMarketplaceListing(Base):
    __tablename__ = "nft_marketplace_listings"
    __table_args__ = {"schema": NFT_MARKETPLACE_SCHEMA_NAME}

    transaction_version: BigIntegerPrimaryKeyType
    index: BigIntegerPrimaryKeyType
    creator_address: StringType
    token_name: StringType
    token_data_id: StringType
    collection: StringType
    collection_id: StringType
    price: NumericType
    token_amount: NumericType
    seller: StringType
    buyer: NullableStringType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    event_type: StringType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType
