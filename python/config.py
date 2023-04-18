import yaml
from pydantic import BaseSettings
from pydantic.env_settings import SettingsSourceCallable


class Config(BaseSettings):
    chain_id: int
    indexer_endpoint: str
    indexer_api_key: str
    starting_version: int
    db_connection_uri: str
    indexer_name: str

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
