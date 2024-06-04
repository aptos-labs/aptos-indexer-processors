use crate::traits::Filterable;
use anyhow::{anyhow, Error};
use aptos_protos::transaction::v1::{
    multisig_transaction_payload, transaction_payload, EntryFunctionId, EntryFunctionPayload,
    TransactionPayload, UserTransactionRequest,
};
use serde::{Deserialize, Serialize};

/// We use this for UserTransactions.
/// We support UserPayload and MultisigPayload
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTransactionRequestFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<UserTransactionPayloadFilter>,
}

impl Filterable<UserTransactionRequest> for UserTransactionRequestFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        if self.sender.is_none() && self.payload.is_none() {
            return Err(Error::msg("At least one of sender or payload must be set"));
        };
        self.payload.is_valid()?;
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &UserTransactionRequest) -> bool {
        if let Some(sender_filter) = &self.sender {
            if &item.sender != sender_filter {
                return false;
            }
        }

        if let Some(payload_filter) = &self.payload {
            // Get the entry_function_payload from both UserPayload and MultisigPayload
            let entry_function_payload = item
                .payload
                .as_ref()
                .and_then(get_entry_function_payload_from_transaction_payload);
            if let Some(payload) = entry_function_payload {
                // Here we have an actual EntryFunctionPayload
                if !payload_filter.is_allowed(payload) {
                    return false;
                }
            }
        }

        true
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EntryFunctionFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
}

impl Filterable<EntryFunctionId> for EntryFunctionFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        if self.address.is_none() && self.module.is_none() && self.function.is_none() {
            return Err(anyhow!(
                "At least one of address, name or function must be set"
            ));
        };
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, module_id: &EntryFunctionId) -> bool {
        if !self.module.is_allowed(&module_id.name) {
            return false;
        }

        if self.address.is_some() || self.function.is_some() {
            if let Some(module) = &module_id.module.as_ref() {
                if !(self.address.is_allowed(&module.address)
                    && self.function.is_allowed(&module.name))
                {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTransactionPayloadFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<EntryFunctionFilter>,
}

impl Filterable<EntryFunctionPayload> for UserTransactionPayloadFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        if self.function.is_none() {
            return Err(Error::msg("At least one of function must be set"));
        };
        self.function.is_valid()?;
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, payload: &EntryFunctionPayload) -> bool {
        self.function.is_allowed_opt(&payload.function)
    }
}

/// Get the entry_function_payload from both UserPayload and MultisigPayload
fn get_entry_function_payload_from_transaction_payload(
    payload: &TransactionPayload,
) -> Option<&EntryFunctionPayload> {
    let z = if let Some(payload) = &payload.payload {
        match payload {
            transaction_payload::Payload::EntryFunctionPayload(ef_payload) => Some(ef_payload),
            transaction_payload::Payload::MultisigPayload(ms_payload) => ms_payload
                .transaction_payload
                .as_ref()
                .and_then(|tp| tp.payload.as_ref())
                .map(|payload| match payload {
                    multisig_transaction_payload::Payload::EntryFunctionPayload(ef_payload) => {
                        ef_payload
                    },
                }),
            _ => None,
        }
    } else {
        None
    };
    z
}
