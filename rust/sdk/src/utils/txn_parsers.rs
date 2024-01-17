use crate::utils::truncate_str;
use aptos_protos::transaction::v1::{
    multisig_transaction_payload::Payload as MultisigPayloadType,
    transaction_payload::Payload as PayloadType, write_set::WriteSet as WriteSetType,
    EntryFunctionId, EntryFunctionPayload, MoveScriptBytecode, MoveType, ScriptPayload,
    TransactionPayload, UserTransactionRequest, WriteSet,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Max length of entry function id string to ensure that DB doesn't explode.
pub const MAX_ENTRY_FUNCTION_LENGTH: usize = 1000;

// Supporting structs to get clean payload without escaped strings

#[derive(Debug, Deserialize, Serialize)]
pub struct EntryFunctionPayloadClean {
    pub function: Option<EntryFunctionId>,
    pub type_arguments: Vec<MoveType>,
    pub arguments: Vec<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ScriptPayloadClean {
    pub code: Option<MoveScriptBytecode>,
    pub type_arguments: Vec<MoveType>,
    pub arguments: Vec<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ScriptWriteSetClean {
    pub execute_as: String,
    pub script: ScriptPayloadClean,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MultisigPayloadClean {
    pub multisig_address: String,
    pub transaction_payload: Option<Value>,
}

pub fn get_entry_function_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    let entry_function_id_str: String = match &user_request.payload.as_ref().unwrap().payload {
        Some(PayloadType::EntryFunctionPayload(payload)) => payload.entry_function_id_str.clone(),
        Some(PayloadType::MultisigPayload(payload)) => {
            if let Some(payload) = payload.transaction_payload.as_ref() {
                match payload.payload.as_ref().unwrap() {
                    MultisigPayloadType::EntryFunctionPayload(payload) => {
                        Some(payload.entry_function_id_str.clone())
                    },
                };
            }
            return None;
        },
        _ => return None,
    };
    Some(truncate_str(
        &entry_function_id_str,
        MAX_ENTRY_FUNCTION_LENGTH,
    ))
}

pub fn get_payload_type(payload: &TransactionPayload) -> String {
    payload.r#type().as_str_name().to_string()
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
/// This function converts the string into json recursively and lets the diesel ORM handles
/// the escaping.
pub fn get_clean_payload(payload: &TransactionPayload, version: i64) -> Option<Value> {
    match payload.payload.as_ref().unwrap() {
        PayloadType::EntryFunctionPayload(inner) => {
            let clean = get_clean_entry_function_payload(inner, version);
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                tracing::error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
        PayloadType::ScriptPayload(inner) => {
            let clean = get_clean_script_payload(inner, version);
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                tracing::error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
        PayloadType::ModuleBundlePayload(inner) => {
            Some(serde_json::to_value(inner).unwrap_or_else(|_| {
                tracing::error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
        PayloadType::WriteSetPayload(inner) => {
            if let Some(writeset) = inner.write_set.as_ref() {
                get_clean_writeset(writeset, version)
            } else {
                None
            }
        },
        PayloadType::MultisigPayload(inner) => {
            let clean = if let Some(payload) = inner.transaction_payload.as_ref() {
                let payload_clean = match payload.payload.as_ref().unwrap() {
                    MultisigPayloadType::EntryFunctionPayload(payload) => {
                        let clean = get_clean_entry_function_payload(payload, version);
                        Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                            tracing::error!(
                                version = version,
                                "Unable to serialize payload into value"
                            );
                            panic!()
                        }))
                    },
                };
                MultisigPayloadClean {
                    multisig_address: inner.multisig_address.clone(),
                    transaction_payload: payload_clean,
                }
            } else {
                MultisigPayloadClean {
                    multisig_address: inner.multisig_address.clone(),
                    transaction_payload: None,
                }
            };
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                tracing::error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
    }
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
/// Note that DirectWriteSet is just events + writeset which is already represented separately
pub fn get_clean_writeset(writeset: &WriteSet, version: i64) -> Option<Value> {
    match writeset.write_set.as_ref().unwrap() {
        WriteSetType::ScriptWriteSet(inner) => {
            let payload = inner.script.as_ref().unwrap();
            Some(
                serde_json::to_value(get_clean_script_payload(payload, version)).unwrap_or_else(
                    |_| {
                        tracing::error!(
                            version = version,
                            "Unable to serialize payload into value"
                        );
                        panic!()
                    },
                ),
            )
        },
        WriteSetType::DirectWriteSet(_) => None,
    }
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
fn get_clean_entry_function_payload(
    payload: &EntryFunctionPayload,
    version: i64,
) -> EntryFunctionPayloadClean {
    EntryFunctionPayloadClean {
        function: payload.function.clone(),
        type_arguments: payload.type_arguments.clone(),
        arguments: payload
            .arguments
            .iter()
            .map(|arg| {
                serde_json::from_str(arg).unwrap_or_else(|_| {
                    tracing::error!(version = version, "Unable to serialize payload into value");
                    panic!()
                })
            })
            .collect(),
    }
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
fn get_clean_script_payload(payload: &ScriptPayload, version: i64) -> ScriptPayloadClean {
    ScriptPayloadClean {
        code: payload.code.clone(),
        type_arguments: payload.type_arguments.clone(),
        arguments: payload
            .arguments
            .iter()
            .map(|arg| {
                serde_json::from_str(arg).unwrap_or_else(|_| {
                    tracing::error!(version = version, "Unable to serialize payload into value");
                    panic!()
                })
            })
            .collect(),
    }
}
