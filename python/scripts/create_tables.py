import argparse

from utils.config import Config
from utils.models.general_models import Base
from processors.example_event_processor import models as example_models
from sqlalchemy import create_engine

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to config file", required=True)
    args = parser.parse_args()

    config = Config.from_yaml_file(args.config)

    engine = create_engine(config.server_config.postgres_connection_string)
    Base.metadata.create_all(engine)
    example_models.Base.metadata.create_all(engine)
