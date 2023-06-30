from dataclasses import dataclass
from enum import Enum
from utils.general_utils import hash, standardize_address, truncate_str

MAX_NAME_LENGTH = 128


class TokenStandard(Enum):
    TOKEN_V1 = "TOKEN_V1"
    TOKEN_V2 = "TOKEN_V2"


class TokenDataIdType:
    def __init__(self, creator: str, collection: str, name: str):
        self.creator = creator
        self.collection = collection
        self.name = name

    def to_hash(self):
        return standardize_address(
            hash(f"{standardize_address(self.creator)}::{self.collection}::{self.name}")
        )

    def get_collection_trunc(self):
        return truncate_str(self.collection, MAX_NAME_LENGTH)

    def get_name_trunc(self):
        return truncate_str(self.name, MAX_NAME_LENGTH)

    def get_collection_data_id_hash(self):
        return CollectionDataIdType(self.creator, self.collection).to_hash()

    def get_creator(self):
        return standardize_address(self.creator)


class CollectionDataIdType:
    def __init__(self, creator: str, name: str):
        self.creator = creator
        self.name = name

    def to_hash(self) -> str:
        return standardize_address(
            hash(f"{standardize_address(self.creator)}::{self.name}")
        )

    def get_name_trunc(self) -> str:
        return truncate_str(self.name, MAX_NAME_LENGTH)

    def get_creator(self):
        return standardize_address(self.creator)


@dataclass
class TokenV2AggregatedData:
    creator_address: str
    collection_address: str
    collection_name: str
    token_name: str
    token_data_id_v1: str | None = None
    token_address_v2: str | None = None


TokenV2AggregatedDataMapping = dict[str, TokenV2AggregatedData]
