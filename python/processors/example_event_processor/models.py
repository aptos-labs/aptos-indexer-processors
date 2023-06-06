from sqlalchemy.orm import DeclarativeBase, Mapped
from utils.models.annotated_types import (
    StringPrimaryKeyType,
    BigIntegerType,
    BigIntegerPrimaryKeyType,
    InsertedAtType,
    TimestampType,
)
from utils.models.general_models import Base


class Event(Base):
    __tablename__ = "events"

    sequence_number: Mapped[BigIntegerPrimaryKeyType]
    creation_number: Mapped[BigIntegerPrimaryKeyType]
    account_address: Mapped[StringPrimaryKeyType]
    transaction_version: Mapped[BigIntegerType]
    transaction_block_height: Mapped[BigIntegerType]
    transaction_timestamp: Mapped[TimestampType]
    type: Mapped[str]
    data: Mapped[str]
    inserted_at: Mapped[InsertedAtType]
    event_index: Mapped[BigIntegerType]
