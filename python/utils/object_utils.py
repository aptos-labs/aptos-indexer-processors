from utils.general_utils import standardize_address
from typing import Optional
from typing_extensions import TypedDict


class ObjectCore(TypedDict):
    allow_ungated_transfer: bool
    guid_creation_num: str
    owner: str


def get_object_core(move_resource_type: str, data: dict) -> Optional[ObjectCore]:
    if move_resource_type != "0x1::object::ObjectCore":
        return None

    return {
        "allow_ungated_transfer": data["allow_ungated_transfer"],
        "guid_creation_num": data["guid_creation_num"],
        "owner": standardize_address(data["owner"]),
    }
