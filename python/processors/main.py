import argparse
import logging
from pythonjsonlogger import jsonlogger
from utils.config import Config
from utils.worker import IndexerProcessorServer
from utils.logging import JsonFormatter, CustomLogger

if __name__ == "__main__":
    # Configure the logger
    logger = CustomLogger("default_python_logger")
    logger.setLevel(logging.INFO)

    # Create a stream handler for stdout
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())

    # Add the stream handler to the logger
    logger.addHandler(stream_handler)
    logging.root = logger

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to config file", required=True)
    args = parser.parse_args()
    config = Config.from_yaml_file(args.config)

    indexer_server = IndexerProcessorServer(
        config,
    )
    indexer_server.run()
