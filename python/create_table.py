from sqlalchemy import BigInteger, Column, create_engine, DateTime, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

import yaml

Base = declarative_base()

class Event(Base):
    __tablename__ = 'events'

    sequence_number = Column(BigInteger, primary_key=True)
    creation_number = Column(BigInteger, primary_key=True)
    account_address = Column(String, primary_key=True)
    transaction_version = Column(BigInteger)
    transaction_block_height = Column(BigInteger)
    type = Column(String)
    data = Column(String)
    inserted_at = Column(DateTime)
    event_index = Column(BigInteger)

with open('../config.yaml', 'r') as file:
    config = yaml.safe_load(file)

engine = create_engine(config['tablename'])
Base.metadata.create_all(engine)