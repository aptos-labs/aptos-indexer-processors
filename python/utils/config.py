import yaml
from utils.models.general_models import NextVersionToProcess
from pydantic import BaseModel, BaseSettings
from pydantic.env_settings import SettingsSourceCallable
from utils.session import Session
from typing import Any, Dict, List, Optional
import logging


class ProcessorConfig(BaseModel):
    type: str


class NFTMarketplaceV2Config(ProcessorConfig):
    marketplace_contract_address: str


class ServerConfig(BaseModel):
    processor_config: NFTMarketplaceV2Config | ProcessorConfig
    indexer_grpc_data_service_address: str
    auth_token: str
    postgres_connection_string: str
    starting_version: Optional[int] = None
    ending_version: Optional[int] = None
    # Used for k8s liveness and readiness probes
    # HTTP2 ping interval in seconds to detect if the connection is still alive
    indexer_grpc_http2_ping_interval_in_secs: int = 30
    # HTTP2 ping timeout in seconds to detect if the connection is still alive
    indexer_grpc_http2_ping_timeout_in_secs: int = 10


class Config(BaseSettings):
    health_check_port: int
    server_config: ServerConfig

    class Config:
        # change order of priority of settings sources such that environment variables take precedence over config file settings
        # inspired by https://docs.pydantic.dev/usage/settings/#changing-priority
        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> tuple[SettingsSourceCallable, ...]:
            return env_settings, init_settings, file_secret_settings

    @classmethod
    def from_yaml_file(cls, path: str):
        with open(path, "r") as file:
            config = yaml.safe_load(file)

        return cls(**config)

    def get_starting_version(self, processor_name: str) -> int:
        next_version_to_process = None

        if self.server_config.postgres_connection_string is not None:
            try:
                with Session() as session, session.begin():
                    next_version_to_process_from_db = session.get(
                        NextVersionToProcess, processor_name
                    )
                    if next_version_to_process_from_db != None:
                        next_version_to_process = (
                            next_version_to_process_from_db.next_version
                        )
            except:
                logging.warn(
                    "[Config] Database error when getting NextVersionToProcess. Skipping..."
                )

        # By default, if nothing is set, start from 0
        starting_version = 0
        if self.server_config.starting_version != None:
            # Start from config's starting_version
            logging.info("[Config] Starting from config starting_version")
            starting_version = self.server_config.starting_version
        elif next_version_to_process != None:
            # Start from next version to process in db
            logging.info("[Config] Starting from version from db")
            starting_version = next_version_to_process
        else:
            logging.info("Starting from version 0")

        return starting_version
