from aptos.util.timestamp import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
MOVE_ABILITY_COPY: MoveAbility
MOVE_ABILITY_DROP: MoveAbility
MOVE_ABILITY_KEY: MoveAbility
MOVE_ABILITY_STORE: MoveAbility
MOVE_ABILITY_UNSPECIFIED: MoveAbility
MOVE_TYPES_ADDRESS: MoveTypes
MOVE_TYPES_BOOL: MoveTypes
MOVE_TYPES_GENERIC_TYPE_PARAM: MoveTypes
MOVE_TYPES_REFERENCE: MoveTypes
MOVE_TYPES_SIGNER: MoveTypes
MOVE_TYPES_STRUCT: MoveTypes
MOVE_TYPES_U128: MoveTypes
MOVE_TYPES_U16: MoveTypes
MOVE_TYPES_U256: MoveTypes
MOVE_TYPES_U32: MoveTypes
MOVE_TYPES_U64: MoveTypes
MOVE_TYPES_U8: MoveTypes
MOVE_TYPES_UNPARSABLE: MoveTypes
MOVE_TYPES_UNSPECIFIED: MoveTypes
MOVE_TYPES_VECTOR: MoveTypes

class AccountSignature(_message.Message):
    __slots__ = ["ed25519", "multi_ed25519", "type"]
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    ED25519_FIELD_NUMBER: _ClassVar[int]
    MULTI_ED25519_FIELD_NUMBER: _ClassVar[int]
    TYPE_ED25519: AccountSignature.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_MULTI_ED25519: AccountSignature.Type
    TYPE_UNSPECIFIED: AccountSignature.Type
    ed25519: Ed25519Signature
    multi_ed25519: MultiEd25519Signature
    type: AccountSignature.Type
    def __init__(self, type: _Optional[_Union[AccountSignature.Type, str]] = ..., ed25519: _Optional[_Union[Ed25519Signature, _Mapping]] = ..., multi_ed25519: _Optional[_Union[MultiEd25519Signature, _Mapping]] = ...) -> None: ...

class Block(_message.Message):
    __slots__ = ["chain_id", "height", "timestamp", "transactions"]
    CHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_FIELD_NUMBER: _ClassVar[int]
    chain_id: int
    height: int
    timestamp: _timestamp_pb2.Timestamp
    transactions: _containers.RepeatedCompositeFieldContainer[Transaction]
    def __init__(self, timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., height: _Optional[int] = ..., transactions: _Optional[_Iterable[_Union[Transaction, _Mapping]]] = ..., chain_id: _Optional[int] = ...) -> None: ...

class BlockMetadataTransaction(_message.Message):
    __slots__ = ["events", "failed_proposer_indices", "id", "previous_block_votes_bitvec", "proposer", "round"]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    FAILED_PROPOSER_INDICES_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_BLOCK_VOTES_BITVEC_FIELD_NUMBER: _ClassVar[int]
    PROPOSER_FIELD_NUMBER: _ClassVar[int]
    ROUND_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[Event]
    failed_proposer_indices: _containers.RepeatedScalarFieldContainer[int]
    id: str
    previous_block_votes_bitvec: bytes
    proposer: str
    round: int
    def __init__(self, id: _Optional[str] = ..., round: _Optional[int] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., previous_block_votes_bitvec: _Optional[bytes] = ..., proposer: _Optional[str] = ..., failed_proposer_indices: _Optional[_Iterable[int]] = ...) -> None: ...

class DeleteModule(_message.Message):
    __slots__ = ["address", "module", "state_key_hash"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    STATE_KEY_HASH_FIELD_NUMBER: _ClassVar[int]
    address: str
    module: MoveModuleId
    state_key_hash: bytes
    def __init__(self, address: _Optional[str] = ..., state_key_hash: _Optional[bytes] = ..., module: _Optional[_Union[MoveModuleId, _Mapping]] = ...) -> None: ...

class DeleteResource(_message.Message):
    __slots__ = ["address", "state_key_hash", "type", "type_str"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    STATE_KEY_HASH_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_STR_FIELD_NUMBER: _ClassVar[int]
    address: str
    state_key_hash: bytes
    type: MoveStructTag
    type_str: str
    def __init__(self, address: _Optional[str] = ..., state_key_hash: _Optional[bytes] = ..., type: _Optional[_Union[MoveStructTag, _Mapping]] = ..., type_str: _Optional[str] = ...) -> None: ...

class DeleteTableData(_message.Message):
    __slots__ = ["key", "key_type"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
    key: str
    key_type: str
    def __init__(self, key: _Optional[str] = ..., key_type: _Optional[str] = ...) -> None: ...

class DeleteTableItem(_message.Message):
    __slots__ = ["data", "handle", "key", "state_key_hash"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    HANDLE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    STATE_KEY_HASH_FIELD_NUMBER: _ClassVar[int]
    data: DeleteTableData
    handle: str
    key: str
    state_key_hash: bytes
    def __init__(self, state_key_hash: _Optional[bytes] = ..., handle: _Optional[str] = ..., key: _Optional[str] = ..., data: _Optional[_Union[DeleteTableData, _Mapping]] = ...) -> None: ...

class DirectWriteSet(_message.Message):
    __slots__ = ["events", "write_set_change"]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    WRITE_SET_CHANGE_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[Event]
    write_set_change: _containers.RepeatedCompositeFieldContainer[WriteSetChange]
    def __init__(self, write_set_change: _Optional[_Iterable[_Union[WriteSetChange, _Mapping]]] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ...) -> None: ...

class Ed25519Signature(_message.Message):
    __slots__ = ["public_key", "signature"]
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    public_key: bytes
    signature: bytes
    def __init__(self, public_key: _Optional[bytes] = ..., signature: _Optional[bytes] = ...) -> None: ...

class EntryFunctionId(_message.Message):
    __slots__ = ["module", "name"]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    module: MoveModuleId
    name: str
    def __init__(self, module: _Optional[_Union[MoveModuleId, _Mapping]] = ..., name: _Optional[str] = ...) -> None: ...

class EntryFunctionPayload(_message.Message):
    __slots__ = ["arguments", "entry_function_id_str", "function", "type_arguments"]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    ENTRY_FUNCTION_ID_STR_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    TYPE_ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    arguments: _containers.RepeatedScalarFieldContainer[str]
    entry_function_id_str: str
    function: EntryFunctionId
    type_arguments: _containers.RepeatedCompositeFieldContainer[MoveType]
    def __init__(self, function: _Optional[_Union[EntryFunctionId, _Mapping]] = ..., type_arguments: _Optional[_Iterable[_Union[MoveType, _Mapping]]] = ..., arguments: _Optional[_Iterable[str]] = ..., entry_function_id_str: _Optional[str] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ["data", "key", "sequence_number", "type", "type_str"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_STR_FIELD_NUMBER: _ClassVar[int]
    data: str
    key: EventKey
    sequence_number: int
    type: MoveType
    type_str: str
    def __init__(self, key: _Optional[_Union[EventKey, _Mapping]] = ..., sequence_number: _Optional[int] = ..., type: _Optional[_Union[MoveType, _Mapping]] = ..., type_str: _Optional[str] = ..., data: _Optional[str] = ...) -> None: ...

class EventKey(_message.Message):
    __slots__ = ["account_address", "creation_number"]
    ACCOUNT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CREATION_NUMBER_FIELD_NUMBER: _ClassVar[int]
    account_address: str
    creation_number: int
    def __init__(self, creation_number: _Optional[int] = ..., account_address: _Optional[str] = ...) -> None: ...

class GenesisTransaction(_message.Message):
    __slots__ = ["events", "payload"]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[Event]
    payload: WriteSet
    def __init__(self, payload: _Optional[_Union[WriteSet, _Mapping]] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ...) -> None: ...

class ModuleBundlePayload(_message.Message):
    __slots__ = ["modules"]
    MODULES_FIELD_NUMBER: _ClassVar[int]
    modules: _containers.RepeatedCompositeFieldContainer[MoveModuleBytecode]
    def __init__(self, modules: _Optional[_Iterable[_Union[MoveModuleBytecode, _Mapping]]] = ...) -> None: ...

class MoveFunction(_message.Message):
    __slots__ = ["generic_type_params", "is_entry", "name", "params", "visibility"]
    class Visibility(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    GENERIC_TYPE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    IS_ENTRY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    RETURN_FIELD_NUMBER: _ClassVar[int]
    VISIBILITY_FIELD_NUMBER: _ClassVar[int]
    VISIBILITY_FRIEND: MoveFunction.Visibility
    VISIBILITY_PRIVATE: MoveFunction.Visibility
    VISIBILITY_PUBLIC: MoveFunction.Visibility
    VISIBILITY_UNSPECIFIED: MoveFunction.Visibility
    generic_type_params: _containers.RepeatedCompositeFieldContainer[MoveFunctionGenericTypeParam]
    is_entry: bool
    name: str
    params: _containers.RepeatedCompositeFieldContainer[MoveType]
    visibility: MoveFunction.Visibility
    def __init__(self, name: _Optional[str] = ..., visibility: _Optional[_Union[MoveFunction.Visibility, str]] = ..., is_entry: bool = ..., generic_type_params: _Optional[_Iterable[_Union[MoveFunctionGenericTypeParam, _Mapping]]] = ..., params: _Optional[_Iterable[_Union[MoveType, _Mapping]]] = ..., **kwargs) -> None: ...

class MoveFunctionGenericTypeParam(_message.Message):
    __slots__ = ["constraints"]
    CONSTRAINTS_FIELD_NUMBER: _ClassVar[int]
    constraints: _containers.RepeatedScalarFieldContainer[MoveAbility]
    def __init__(self, constraints: _Optional[_Iterable[_Union[MoveAbility, str]]] = ...) -> None: ...

class MoveModule(_message.Message):
    __slots__ = ["address", "exposed_functions", "friends", "name", "structs"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    EXPOSED_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    FRIENDS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STRUCTS_FIELD_NUMBER: _ClassVar[int]
    address: str
    exposed_functions: _containers.RepeatedCompositeFieldContainer[MoveFunction]
    friends: _containers.RepeatedCompositeFieldContainer[MoveModuleId]
    name: str
    structs: _containers.RepeatedCompositeFieldContainer[MoveStruct]
    def __init__(self, address: _Optional[str] = ..., name: _Optional[str] = ..., friends: _Optional[_Iterable[_Union[MoveModuleId, _Mapping]]] = ..., exposed_functions: _Optional[_Iterable[_Union[MoveFunction, _Mapping]]] = ..., structs: _Optional[_Iterable[_Union[MoveStruct, _Mapping]]] = ...) -> None: ...

class MoveModuleBytecode(_message.Message):
    __slots__ = ["abi", "bytecode"]
    ABI_FIELD_NUMBER: _ClassVar[int]
    BYTECODE_FIELD_NUMBER: _ClassVar[int]
    abi: MoveModule
    bytecode: bytes
    def __init__(self, bytecode: _Optional[bytes] = ..., abi: _Optional[_Union[MoveModule, _Mapping]] = ...) -> None: ...

class MoveModuleId(_message.Message):
    __slots__ = ["address", "name"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    address: str
    name: str
    def __init__(self, address: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class MoveScriptBytecode(_message.Message):
    __slots__ = ["abi", "bytecode"]
    ABI_FIELD_NUMBER: _ClassVar[int]
    BYTECODE_FIELD_NUMBER: _ClassVar[int]
    abi: MoveFunction
    bytecode: bytes
    def __init__(self, bytecode: _Optional[bytes] = ..., abi: _Optional[_Union[MoveFunction, _Mapping]] = ...) -> None: ...

class MoveStruct(_message.Message):
    __slots__ = ["abilities", "fields", "generic_type_params", "is_native", "name"]
    ABILITIES_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    GENERIC_TYPE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    IS_NATIVE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    abilities: _containers.RepeatedScalarFieldContainer[MoveAbility]
    fields: _containers.RepeatedCompositeFieldContainer[MoveStructField]
    generic_type_params: _containers.RepeatedCompositeFieldContainer[MoveStructGenericTypeParam]
    is_native: bool
    name: str
    def __init__(self, name: _Optional[str] = ..., is_native: bool = ..., abilities: _Optional[_Iterable[_Union[MoveAbility, str]]] = ..., generic_type_params: _Optional[_Iterable[_Union[MoveStructGenericTypeParam, _Mapping]]] = ..., fields: _Optional[_Iterable[_Union[MoveStructField, _Mapping]]] = ...) -> None: ...

class MoveStructField(_message.Message):
    __slots__ = ["name", "type"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: MoveType
    def __init__(self, name: _Optional[str] = ..., type: _Optional[_Union[MoveType, _Mapping]] = ...) -> None: ...

class MoveStructGenericTypeParam(_message.Message):
    __slots__ = ["constraints", "is_phantom"]
    CONSTRAINTS_FIELD_NUMBER: _ClassVar[int]
    IS_PHANTOM_FIELD_NUMBER: _ClassVar[int]
    constraints: _containers.RepeatedScalarFieldContainer[MoveAbility]
    is_phantom: bool
    def __init__(self, constraints: _Optional[_Iterable[_Union[MoveAbility, str]]] = ..., is_phantom: bool = ...) -> None: ...

class MoveStructTag(_message.Message):
    __slots__ = ["address", "generic_type_params", "module", "name"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    GENERIC_TYPE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    address: str
    generic_type_params: _containers.RepeatedCompositeFieldContainer[MoveType]
    module: str
    name: str
    def __init__(self, address: _Optional[str] = ..., module: _Optional[str] = ..., name: _Optional[str] = ..., generic_type_params: _Optional[_Iterable[_Union[MoveType, _Mapping]]] = ...) -> None: ...

class MoveType(_message.Message):
    __slots__ = ["generic_type_param_index", "reference", "struct", "type", "unparsable", "vector"]
    class ReferenceType(_message.Message):
        __slots__ = ["mutable", "to"]
        MUTABLE_FIELD_NUMBER: _ClassVar[int]
        TO_FIELD_NUMBER: _ClassVar[int]
        mutable: bool
        to: MoveType
        def __init__(self, mutable: bool = ..., to: _Optional[_Union[MoveType, _Mapping]] = ...) -> None: ...
    GENERIC_TYPE_PARAM_INDEX_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    STRUCT_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    UNPARSABLE_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    generic_type_param_index: int
    reference: MoveType.ReferenceType
    struct: MoveStructTag
    type: MoveTypes
    unparsable: str
    vector: MoveType
    def __init__(self, type: _Optional[_Union[MoveTypes, str]] = ..., vector: _Optional[_Union[MoveType, _Mapping]] = ..., struct: _Optional[_Union[MoveStructTag, _Mapping]] = ..., generic_type_param_index: _Optional[int] = ..., reference: _Optional[_Union[MoveType.ReferenceType, _Mapping]] = ..., unparsable: _Optional[str] = ...) -> None: ...

class MultiAgentSignature(_message.Message):
    __slots__ = ["secondary_signer_addresses", "secondary_signers", "sender"]
    SECONDARY_SIGNERS_FIELD_NUMBER: _ClassVar[int]
    SECONDARY_SIGNER_ADDRESSES_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    secondary_signer_addresses: _containers.RepeatedScalarFieldContainer[str]
    secondary_signers: _containers.RepeatedCompositeFieldContainer[AccountSignature]
    sender: AccountSignature
    def __init__(self, sender: _Optional[_Union[AccountSignature, _Mapping]] = ..., secondary_signer_addresses: _Optional[_Iterable[str]] = ..., secondary_signers: _Optional[_Iterable[_Union[AccountSignature, _Mapping]]] = ...) -> None: ...

class MultiEd25519Signature(_message.Message):
    __slots__ = ["public_key_indices", "public_keys", "signatures", "threshold"]
    PUBLIC_KEYS_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_INDICES_FIELD_NUMBER: _ClassVar[int]
    SIGNATURES_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    public_key_indices: _containers.RepeatedScalarFieldContainer[int]
    public_keys: _containers.RepeatedScalarFieldContainer[bytes]
    signatures: _containers.RepeatedScalarFieldContainer[bytes]
    threshold: int
    def __init__(self, public_keys: _Optional[_Iterable[bytes]] = ..., signatures: _Optional[_Iterable[bytes]] = ..., threshold: _Optional[int] = ..., public_key_indices: _Optional[_Iterable[int]] = ...) -> None: ...

class MultisigPayload(_message.Message):
    __slots__ = ["multisig_address", "transaction_payload"]
    MULTISIG_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    multisig_address: str
    transaction_payload: MultisigTransactionPayload
    def __init__(self, multisig_address: _Optional[str] = ..., transaction_payload: _Optional[_Union[MultisigTransactionPayload, _Mapping]] = ...) -> None: ...

class MultisigTransactionPayload(_message.Message):
    __slots__ = ["entry_function_payload", "type"]
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    ENTRY_FUNCTION_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TYPE_ENTRY_FUNCTION_PAYLOAD: MultisigTransactionPayload.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_UNSPECIFIED: MultisigTransactionPayload.Type
    entry_function_payload: EntryFunctionPayload
    type: MultisigTransactionPayload.Type
    def __init__(self, type: _Optional[_Union[MultisigTransactionPayload.Type, str]] = ..., entry_function_payload: _Optional[_Union[EntryFunctionPayload, _Mapping]] = ...) -> None: ...

class ScriptPayload(_message.Message):
    __slots__ = ["arguments", "code", "type_arguments"]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    TYPE_ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    arguments: _containers.RepeatedScalarFieldContainer[str]
    code: MoveScriptBytecode
    type_arguments: _containers.RepeatedCompositeFieldContainer[MoveType]
    def __init__(self, code: _Optional[_Union[MoveScriptBytecode, _Mapping]] = ..., type_arguments: _Optional[_Iterable[_Union[MoveType, _Mapping]]] = ..., arguments: _Optional[_Iterable[str]] = ...) -> None: ...

class ScriptWriteSet(_message.Message):
    __slots__ = ["execute_as", "script"]
    EXECUTE_AS_FIELD_NUMBER: _ClassVar[int]
    SCRIPT_FIELD_NUMBER: _ClassVar[int]
    execute_as: str
    script: ScriptPayload
    def __init__(self, execute_as: _Optional[str] = ..., script: _Optional[_Union[ScriptPayload, _Mapping]] = ...) -> None: ...

class Signature(_message.Message):
    __slots__ = ["ed25519", "multi_agent", "multi_ed25519", "type"]
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    ED25519_FIELD_NUMBER: _ClassVar[int]
    MULTI_AGENT_FIELD_NUMBER: _ClassVar[int]
    MULTI_ED25519_FIELD_NUMBER: _ClassVar[int]
    TYPE_ED25519: Signature.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_MULTI_AGENT: Signature.Type
    TYPE_MULTI_ED25519: Signature.Type
    TYPE_UNSPECIFIED: Signature.Type
    ed25519: Ed25519Signature
    multi_agent: MultiAgentSignature
    multi_ed25519: MultiEd25519Signature
    type: Signature.Type
    def __init__(self, type: _Optional[_Union[Signature.Type, str]] = ..., ed25519: _Optional[_Union[Ed25519Signature, _Mapping]] = ..., multi_ed25519: _Optional[_Union[MultiEd25519Signature, _Mapping]] = ..., multi_agent: _Optional[_Union[MultiAgentSignature, _Mapping]] = ...) -> None: ...

class StateCheckpointTransaction(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Transaction(_message.Message):
    __slots__ = ["block_height", "block_metadata", "epoch", "genesis", "info", "state_checkpoint", "timestamp", "type", "user", "version"]
    class TransactionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    BLOCK_METADATA_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    GENESIS_FIELD_NUMBER: _ClassVar[int]
    INFO_FIELD_NUMBER: _ClassVar[int]
    STATE_CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_TYPE_BLOCK_METADATA: Transaction.TransactionType
    TRANSACTION_TYPE_GENESIS: Transaction.TransactionType
    TRANSACTION_TYPE_STATE_CHECKPOINT: Transaction.TransactionType
    TRANSACTION_TYPE_UNSPECIFIED: Transaction.TransactionType
    TRANSACTION_TYPE_USER: Transaction.TransactionType
    TYPE_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    block_height: int
    block_metadata: BlockMetadataTransaction
    epoch: int
    genesis: GenesisTransaction
    info: TransactionInfo
    state_checkpoint: StateCheckpointTransaction
    timestamp: _timestamp_pb2.Timestamp
    type: Transaction.TransactionType
    user: UserTransaction
    version: int
    def __init__(self, timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., version: _Optional[int] = ..., info: _Optional[_Union[TransactionInfo, _Mapping]] = ..., epoch: _Optional[int] = ..., block_height: _Optional[int] = ..., type: _Optional[_Union[Transaction.TransactionType, str]] = ..., block_metadata: _Optional[_Union[BlockMetadataTransaction, _Mapping]] = ..., genesis: _Optional[_Union[GenesisTransaction, _Mapping]] = ..., state_checkpoint: _Optional[_Union[StateCheckpointTransaction, _Mapping]] = ..., user: _Optional[_Union[UserTransaction, _Mapping]] = ...) -> None: ...

class TransactionInfo(_message.Message):
    __slots__ = ["accumulator_root_hash", "changes", "event_root_hash", "gas_used", "hash", "state_change_hash", "state_checkpoint_hash", "success", "vm_status"]
    ACCUMULATOR_ROOT_HASH_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    EVENT_ROOT_HASH_FIELD_NUMBER: _ClassVar[int]
    GAS_USED_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    STATE_CHANGE_HASH_FIELD_NUMBER: _ClassVar[int]
    STATE_CHECKPOINT_HASH_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    VM_STATUS_FIELD_NUMBER: _ClassVar[int]
    accumulator_root_hash: bytes
    changes: _containers.RepeatedCompositeFieldContainer[WriteSetChange]
    event_root_hash: bytes
    gas_used: int
    hash: bytes
    state_change_hash: bytes
    state_checkpoint_hash: bytes
    success: bool
    vm_status: str
    def __init__(self, hash: _Optional[bytes] = ..., state_change_hash: _Optional[bytes] = ..., event_root_hash: _Optional[bytes] = ..., state_checkpoint_hash: _Optional[bytes] = ..., gas_used: _Optional[int] = ..., success: bool = ..., vm_status: _Optional[str] = ..., accumulator_root_hash: _Optional[bytes] = ..., changes: _Optional[_Iterable[_Union[WriteSetChange, _Mapping]]] = ...) -> None: ...

class TransactionPayload(_message.Message):
    __slots__ = ["entry_function_payload", "module_bundle_payload", "multisig_payload", "script_payload", "type", "write_set_payload"]
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    ENTRY_FUNCTION_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    MODULE_BUNDLE_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    MULTISIG_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SCRIPT_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TYPE_ENTRY_FUNCTION_PAYLOAD: TransactionPayload.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_MODULE_BUNDLE_PAYLOAD: TransactionPayload.Type
    TYPE_MULTISIG_PAYLOAD: TransactionPayload.Type
    TYPE_SCRIPT_PAYLOAD: TransactionPayload.Type
    TYPE_UNSPECIFIED: TransactionPayload.Type
    TYPE_WRITE_SET_PAYLOAD: TransactionPayload.Type
    WRITE_SET_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    entry_function_payload: EntryFunctionPayload
    module_bundle_payload: ModuleBundlePayload
    multisig_payload: MultisigPayload
    script_payload: ScriptPayload
    type: TransactionPayload.Type
    write_set_payload: WriteSetPayload
    def __init__(self, type: _Optional[_Union[TransactionPayload.Type, str]] = ..., entry_function_payload: _Optional[_Union[EntryFunctionPayload, _Mapping]] = ..., script_payload: _Optional[_Union[ScriptPayload, _Mapping]] = ..., module_bundle_payload: _Optional[_Union[ModuleBundlePayload, _Mapping]] = ..., write_set_payload: _Optional[_Union[WriteSetPayload, _Mapping]] = ..., multisig_payload: _Optional[_Union[MultisigPayload, _Mapping]] = ...) -> None: ...

class UserTransaction(_message.Message):
    __slots__ = ["events", "request"]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[Event]
    request: UserTransactionRequest
    def __init__(self, request: _Optional[_Union[UserTransactionRequest, _Mapping]] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ...) -> None: ...

class UserTransactionRequest(_message.Message):
    __slots__ = ["expiration_timestamp_secs", "gas_unit_price", "max_gas_amount", "payload", "sender", "sequence_number", "signature"]
    EXPIRATION_TIMESTAMP_SECS_FIELD_NUMBER: _ClassVar[int]
    GAS_UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    MAX_GAS_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    expiration_timestamp_secs: _timestamp_pb2.Timestamp
    gas_unit_price: int
    max_gas_amount: int
    payload: TransactionPayload
    sender: str
    sequence_number: int
    signature: Signature
    def __init__(self, sender: _Optional[str] = ..., sequence_number: _Optional[int] = ..., max_gas_amount: _Optional[int] = ..., gas_unit_price: _Optional[int] = ..., expiration_timestamp_secs: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., payload: _Optional[_Union[TransactionPayload, _Mapping]] = ..., signature: _Optional[_Union[Signature, _Mapping]] = ...) -> None: ...

class WriteModule(_message.Message):
    __slots__ = ["address", "data", "state_key_hash"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    STATE_KEY_HASH_FIELD_NUMBER: _ClassVar[int]
    address: str
    data: MoveModuleBytecode
    state_key_hash: bytes
    def __init__(self, address: _Optional[str] = ..., state_key_hash: _Optional[bytes] = ..., data: _Optional[_Union[MoveModuleBytecode, _Mapping]] = ...) -> None: ...

class WriteResource(_message.Message):
    __slots__ = ["address", "data", "state_key_hash", "type", "type_str"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    STATE_KEY_HASH_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_STR_FIELD_NUMBER: _ClassVar[int]
    address: str
    data: str
    state_key_hash: bytes
    type: MoveStructTag
    type_str: str
    def __init__(self, address: _Optional[str] = ..., state_key_hash: _Optional[bytes] = ..., type: _Optional[_Union[MoveStructTag, _Mapping]] = ..., type_str: _Optional[str] = ..., data: _Optional[str] = ...) -> None: ...

class WriteSet(_message.Message):
    __slots__ = ["direct_write_set", "script_write_set", "write_set_type"]
    class WriteSetType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    DIRECT_WRITE_SET_FIELD_NUMBER: _ClassVar[int]
    SCRIPT_WRITE_SET_FIELD_NUMBER: _ClassVar[int]
    WRITE_SET_TYPE_DIRECT_WRITE_SET: WriteSet.WriteSetType
    WRITE_SET_TYPE_FIELD_NUMBER: _ClassVar[int]
    WRITE_SET_TYPE_SCRIPT_WRITE_SET: WriteSet.WriteSetType
    WRITE_SET_TYPE_UNSPECIFIED: WriteSet.WriteSetType
    direct_write_set: DirectWriteSet
    script_write_set: ScriptWriteSet
    write_set_type: WriteSet.WriteSetType
    def __init__(self, write_set_type: _Optional[_Union[WriteSet.WriteSetType, str]] = ..., script_write_set: _Optional[_Union[ScriptWriteSet, _Mapping]] = ..., direct_write_set: _Optional[_Union[DirectWriteSet, _Mapping]] = ...) -> None: ...

class WriteSetChange(_message.Message):
    __slots__ = ["delete_module", "delete_resource", "delete_table_item", "type", "write_module", "write_resource", "write_table_item"]
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    DELETE_MODULE_FIELD_NUMBER: _ClassVar[int]
    DELETE_RESOURCE_FIELD_NUMBER: _ClassVar[int]
    DELETE_TABLE_ITEM_FIELD_NUMBER: _ClassVar[int]
    TYPE_DELETE_MODULE: WriteSetChange.Type
    TYPE_DELETE_RESOURCE: WriteSetChange.Type
    TYPE_DELETE_TABLE_ITEM: WriteSetChange.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_UNSPECIFIED: WriteSetChange.Type
    TYPE_WRITE_MODULE: WriteSetChange.Type
    TYPE_WRITE_RESOURCE: WriteSetChange.Type
    TYPE_WRITE_TABLE_ITEM: WriteSetChange.Type
    WRITE_MODULE_FIELD_NUMBER: _ClassVar[int]
    WRITE_RESOURCE_FIELD_NUMBER: _ClassVar[int]
    WRITE_TABLE_ITEM_FIELD_NUMBER: _ClassVar[int]
    delete_module: DeleteModule
    delete_resource: DeleteResource
    delete_table_item: DeleteTableItem
    type: WriteSetChange.Type
    write_module: WriteModule
    write_resource: WriteResource
    write_table_item: WriteTableItem
    def __init__(self, type: _Optional[_Union[WriteSetChange.Type, str]] = ..., delete_module: _Optional[_Union[DeleteModule, _Mapping]] = ..., delete_resource: _Optional[_Union[DeleteResource, _Mapping]] = ..., delete_table_item: _Optional[_Union[DeleteTableItem, _Mapping]] = ..., write_module: _Optional[_Union[WriteModule, _Mapping]] = ..., write_resource: _Optional[_Union[WriteResource, _Mapping]] = ..., write_table_item: _Optional[_Union[WriteTableItem, _Mapping]] = ...) -> None: ...

class WriteSetPayload(_message.Message):
    __slots__ = ["write_set"]
    WRITE_SET_FIELD_NUMBER: _ClassVar[int]
    write_set: WriteSet
    def __init__(self, write_set: _Optional[_Union[WriteSet, _Mapping]] = ...) -> None: ...

class WriteTableData(_message.Message):
    __slots__ = ["key", "key_type", "value", "value_type"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    key: str
    key_type: str
    value: str
    value_type: str
    def __init__(self, key: _Optional[str] = ..., key_type: _Optional[str] = ..., value: _Optional[str] = ..., value_type: _Optional[str] = ...) -> None: ...

class WriteTableItem(_message.Message):
    __slots__ = ["data", "handle", "key", "state_key_hash"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    HANDLE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    STATE_KEY_HASH_FIELD_NUMBER: _ClassVar[int]
    data: WriteTableData
    handle: str
    key: str
    state_key_hash: bytes
    def __init__(self, state_key_hash: _Optional[bytes] = ..., handle: _Optional[str] = ..., key: _Optional[str] = ..., data: _Optional[_Union[WriteTableData, _Mapping]] = ...) -> None: ...

class MoveTypes(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class MoveAbility(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
