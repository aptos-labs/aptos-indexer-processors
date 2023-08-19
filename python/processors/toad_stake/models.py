from utils.models.annotated_types import StringType
from utils.models.annotated_types import (
    BooleanType,
    StringPrimaryKeyType,
    BigIntegerType,
    BigIntegerPrimaryKeyType,
    SmallIntegerPrimaryKeyType,
    InsertedAtType,
    TimestampType,
    NumericType,
)
from utils.models.general_models import Base
from utils.models.schema_names import TOAD_STAKE_SCHEMA_NAME


class ToadStakeEvent(Base):
    __tablename__ = "toad_stake_events"
    __table_args__ = ({"schema": TOAD_STAKE_SCHEMA_NAME},)

    sequence_number: BigIntegerPrimaryKeyType
    creation_number: BigIntegerPrimaryKeyType
    account_address: StringPrimaryKeyType
    toad_id: SmallIntegerPrimaryKeyType
    is_staked: BooleanType
    total_rewards_claimed: BigIntegerType
    last_event_type: StringType
    last_staking_initiated_timestamp: TimestampType
    last_rewards_claimed_timestamp: TimestampType
    last_unstaked_timestamp: TimestampType
    transaction_version: BigIntegerType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType
    event_index: BigIntegerType
