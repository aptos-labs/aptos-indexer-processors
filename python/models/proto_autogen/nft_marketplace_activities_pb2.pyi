from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class NFTMarketplaceActivityRow(_message.Message):
    __slots__ = [
        "_CHANGE_TYPE",
        "amount",
        "buyer",
        "collection",
        "collection_id",
        "contract_address",
        "creator_address",
        "entry_function_id_str",
        "event_index",
        "event_type",
        "json_data",
        "marketplace",
        "price",
        "seller",
        "standard_event_type",
        "token_data_id",
        "token_name",
        "transaction_timestamp",
        "transaction_version",
    ]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    BUYER_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CREATOR_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    ENTRY_FUNCTION_ID_STR_FIELD_NUMBER: _ClassVar[int]
    EVENT_INDEX_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    JSON_DATA_FIELD_NUMBER: _ClassVar[int]
    MARKETPLACE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    SELLER_FIELD_NUMBER: _ClassVar[int]
    STANDARD_EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TOKEN_DATA_ID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    _CHANGE_TYPE: str
    _CHANGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    amount: int
    buyer: str
    collection: str
    collection_id: str
    contract_address: str
    creator_address: str
    entry_function_id_str: str
    event_index: int
    event_type: str
    json_data: str
    marketplace: str
    price: float
    seller: str
    standard_event_type: str
    token_data_id: str
    token_name: str
    transaction_timestamp: int
    transaction_version: int
    def __init__(
        self,
        transaction_version: _Optional[int] = ...,
        event_index: _Optional[int] = ...,
        event_type: _Optional[str] = ...,
        standard_event_type: _Optional[str] = ...,
        creator_address: _Optional[str] = ...,
        collection: _Optional[str] = ...,
        token_name: _Optional[str] = ...,
        token_data_id: _Optional[str] = ...,
        collection_id: _Optional[str] = ...,
        price: _Optional[float] = ...,
        amount: _Optional[int] = ...,
        buyer: _Optional[str] = ...,
        seller: _Optional[str] = ...,
        json_data: _Optional[str] = ...,
        marketplace: _Optional[str] = ...,
        contract_address: _Optional[str] = ...,
        entry_function_id_str: _Optional[str] = ...,
        transaction_timestamp: _Optional[int] = ...,
        _CHANGE_TYPE: _Optional[str] = ...,
    ) -> None: ...
