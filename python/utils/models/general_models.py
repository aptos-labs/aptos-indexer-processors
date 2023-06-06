from sqlalchemy import BigInteger, create_engine, DateTime, func, String
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from datetime import datetime
from typing_extensions import Annotated
from utils.models.annotated_types import (
    StringPrimaryKeyType,
    BigIntegerType,
    UpdatedAtType,
)


class Base(DeclarativeBase):
    pass


class NextVersionToProcess(Base):
    __tablename__ = "next_versions_to_process"

    indexer_name: Mapped[StringPrimaryKeyType]
    next_version: Mapped[BigIntegerType]
    updated_at: Mapped[UpdatedAtType]
