from sqlalchemy import BigInteger, create_engine, DateTime, func, String
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from datetime import datetime
from typing_extensions import Annotated
import argparse
from utils.config import Config


class Base(DeclarativeBase):
    pass


bigintPkType = Annotated[int, mapped_column(BigInteger, primary_key=True)]
stringPkType = Annotated[str, mapped_column(String, primary_key=True)]
bigintType = Annotated[int, mapped_column(BigInteger)]
insertedAtType = Annotated[datetime, mapped_column(DateTime(timezone=True))]
updatedAtType = Annotated[
    datetime,
    mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        onupdate=func.now(),
    ),
]


class Event(Base):
    __tablename__ = "events"

    sequence_number: Mapped[bigintPkType]
    creation_number: Mapped[bigintPkType]
    account_address: Mapped[stringPkType]
    transaction_version: Mapped[bigintType]
    transaction_block_height: Mapped[bigintType]
    type: Mapped[str]
    data: Mapped[str]
    inserted_at: Mapped[insertedAtType]
    event_index: Mapped[bigintType]


class NextVersionToProcess(Base):
    __tablename__ = "next_versions_to_process"

    indexer_name: Mapped[stringPkType]
    next_version: Mapped[bigintType]
    updated_at: Mapped[updatedAtType]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to config file", required=True)
    args = parser.parse_args()

    config = Config.from_yaml_file(args.config)

    engine = create_engine(config.db_connection_uri)
    Base.metadata.create_all(engine)
