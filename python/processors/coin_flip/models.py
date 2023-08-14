from utils.models.annotated_types import StringType
from utils.models.annotated_types import (
    BooleanType,
    StringPrimaryKeyType,
    BigIntegerType,
    BigIntegerPrimaryKeyType,
    InsertedAtType,
    TimestampType,
    NumericType,
)
from utils.models.general_models import Base
from utils.models.schema_names import COIN_FLIP_SCHEMA_NAME


class CoinFlipEvent(Base):
    __tablename__ = "coin_flip_events"
    __table_args__ = ({"schema": COIN_FLIP_SCHEMA_NAME},)

    sequence_number: BigIntegerPrimaryKeyType
    creation_number: BigIntegerPrimaryKeyType
    account_address: StringPrimaryKeyType
    prediction: BooleanType
    result: BooleanType
    wins: BigIntegerType
    losses: BigIntegerType
    win_percentage: NumericType
    transaction_version: BigIntegerType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType
    event_index: BigIntegerType
