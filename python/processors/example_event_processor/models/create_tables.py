import argparse

from utils.config import Config
from processors.example_event_processor.models.models import Base
from sqlalchemy import create_engine

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to config file", required=True)
    args = parser.parse_args()

    config = Config.from_yaml_file(args.config)

    engine = create_engine(config.db_connection_uri)
    Base.metadata.create_all(engine)
