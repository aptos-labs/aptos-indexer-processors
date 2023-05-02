from sqlalchemy import BigInteger, create_engine, DateTime, func, String
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from datetime import datetime
import argparse
from config import Config


class Base(DeclarativeBase):
    pass


parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="Path to config file", required=True)
args = parser.parse_args()

config = Config.from_yaml_file(args.config)


class Event(Base):
    __tablename__ = "events"

    sequence_number: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    creation_number: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    account_address: Mapped[str] = mapped_column(String, primary_key=True)
    transaction_version: Mapped[int] = mapped_column(BigInteger)
    transaction_block_height: Mapped[int] = mapped_column(BigInteger)
    type: Mapped[str]
    data: Mapped[str]
    inserted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    event_index: Mapped[int] = mapped_column(BigInteger)


class NextVersionToProcess(Base):
    __tablename__ = "next_versions_to_process"

    indexer_name: Mapped[str] = mapped_column(primary_key=True)
    next_version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        onupdate=func.now(),
    )


engine = create_engine(config.db_connection_uri)
Base.metadata.create_all(engine)
