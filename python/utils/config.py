import yaml

from utils.models.general_models import NextVersionToProcess
from pydantic import BaseModel, BaseSettings
from pydantic.env_settings import SettingsSourceCallable
from utils.session import Session
from typing import Any, Dict, List, Optional


class Config(BaseSettings):
    processor_name: str
    grpc_data_stream_endpoint: str
    grpc_data_stream_api_key: str
    db_connection_uri: str
    # Used for k8s liveness and readiness probes
    health_port: int
    # HTTP2 ping interval in seconds to detect if the connection is still alive
    indexer_grpc_http2_ping_interval_in_secs: int
    # HTTP2 ping timeout in seconds to detect if the connection is still alive
    indexer_grpc_http2_ping_timeout_in_secs: int
    starting_version_default: Optional[int] = None
    starting_version_backfill: Optional[int] = None
    ending_version: Optional[int] = None
    # Custom config variables for each processor
    custom_config: Optional[Dict[str, Any]] = None

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

        if self.db_connection_uri is not None:
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
                print("Database error when getting NextVersionToProcess. Skipping...")

        # By default, if nothing is set, start from 0
        starting_version = 0
        if self.starting_version_backfill != None:
            # Start from config's starting_version_backfill if set
            print("[Config] Starting from starting_version_backfill")
            starting_version = self.starting_version_backfill
        elif next_version_to_process != None:
            # Start from next version to process in db
            print("[Config] Starting from version from db")
            starting_version = next_version_to_process
        elif self.starting_version_default != None:
            # Start from config's starting_version_default if set
            print("[Config] Starting from starting_version_default")
            starting_version = self.starting_version_default
        else:
            print("Starting from version 0")

        return starting_version
