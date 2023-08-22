import argparse
from utils.config import Config
from utils.worker import IndexerProcessorServer
import asyncio

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to config file", required=True)
    args = parser.parse_args()
    config = Config.from_yaml_file(args.config)

    indexer_server = IndexerProcessorServer(
        config,
    )
    asyncio.run(indexer_server.run())
