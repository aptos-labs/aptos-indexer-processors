from utils.models.annotated_types import (
    StringType,
    BigIntegerPrimaryKeyType,
    InsertedAtType,
    JsonType,
    NullableNumericType,
    NullableStringType,
    TimestampType,
)
from utils.models.general_models import Base


class NFTMarketplaceEvent(Base):
    __tablename__ = "nft_marketplace_activities"
    __table_args__ = {"schema": "nft_marketplace"}

    transaction_version: BigIntegerPrimaryKeyType
    event_index: BigIntegerPrimaryKeyType
    event_type: StringType
    standard_event_type: StringType
    creator_address: NullableStringType
    collection: NullableStringType
    token_name: NullableStringType
    token_data_id: NullableStringType
    collection_id: NullableStringType
    price: NullableNumericType
    token_amount: NullableNumericType
    buyer: NullableStringType
    seller: NullableStringType
    json_data: JsonType
    marketplace: StringType
    contract_address: StringType
    entry_function_id_str: StringType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType
