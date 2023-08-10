from utils.models.annotated_types import StringType
from utils.models.annotated_types import (
    StringPrimaryKeyType,
    BigIntegerType,
    BigIntegerPrimaryKeyType,
    InsertedAtType,
    TimestampType,
)
from utils.models.general_models import Base
from utils.models.schema_names import EXAMPLE


class Event(Base):
    __tablename__ = "events"
    __table_args__ = ({"schema": EXAMPLE},)

    sequence_number: BigIntegerPrimaryKeyType
    creation_number: BigIntegerPrimaryKeyType
    account_address: StringPrimaryKeyType
    transaction_version: BigIntegerType
    transaction_block_height: BigIntegerType
    transaction_timestamp: TimestampType
    type: StringType
    data: StringType
    inserted_at: InsertedAtType
    event_index: BigIntegerType
