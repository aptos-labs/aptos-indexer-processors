from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from datetime import datetime
from typing_extensions import Annotated
from utils.models.annotated_types import (
    StringPrimaryKeyType,
    BigIntegerType,
    UpdatedAtType,
    BooleanType,
)


class Base(DeclarativeBase):
    pass


class NextVersionToProcess(Base):
    __tablename__ = "next_versions_to_process"
    __table_args__ = {"schema": "per_schema"}

    indexer_name: StringPrimaryKeyType
    next_version: BigIntegerType
    updated_at: UpdatedAtType
