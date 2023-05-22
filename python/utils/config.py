import yaml

from processors.example_event_processor.models.models import NextVersionToProcess
from pydantic import BaseModel, BaseSettings
from pydantic.env_settings import SettingsSourceCallable
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Optional


class Config(BaseSettings):
    chain_id: int
    indexer_endpoint: str
    indexer_api_key: str
    db_connection_uri: str
    starting_version_default: Optional[int] = None
    starting_version_override: Optional[int] = None

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

    def get_starting_version(self, indexer_name: str) -> int:
        # Get next version to process from db
        next_version = None
        if self.db_connection_uri != None:
            if self.db_connection_uri.startswith("postgresql://"):
                engine = create_engine(self.db_connection_uri)

                with Session(engine) as session, session.begin():
                    next_version_to_process = session.get(
                        NextVersionToProcess, indexer_name
                    )
                    if next_version_to_process != None:
                        next_version = next_version_to_process.next_version

        # By default, if nothing is set, start from 0
        starting_version = 0
        if self.starting_version_override != None:
            # Start from config's starting_version_override if set
            starting_version = self.starting_version_override
        elif next_version != None:
            # Start from next version to process in db
            starting_version = next_version
        elif self.starting_version_default != None:
            # Start from config's starting_version_default if set
            starting_version = self.starting_version_default

        return starting_version
