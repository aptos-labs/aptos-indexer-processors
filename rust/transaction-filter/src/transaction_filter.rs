use aptos_protos::transaction::v1::{
    transaction::{TransactionType, TxnData},
    transaction_payload::Payload,
    Transaction,
};
use serde::{Deserialize, Serialize};

/// Allows filtering transactions based on various criteria
/// The criteria are combined with `AND`
/// If a criteria is not set, it is ignored
/// Criteria will be loaded from the config file
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct TransactionFilter {
    // Only allow transactions from these contract addresses
    focus_contract_addresses: Option<ahash::HashSet<String>>,
    // Skip transactions from these sender addresses
    skip_sender_addresses: Option<ahash::HashSet<String>>,
    // Skip all transactions that aren't user transactions
    focus_user_transactions: bool,
}

impl TransactionFilter {
    pub fn new(
        focus_contract_addresses: Option<ahash::HashSet<String>>,
        skip_sender_addresses: Option<ahash::HashSet<String>>,
        focus_user_transactions: bool,
    ) -> Self {
        // TODO: normalize addresses
        Self {
            focus_contract_addresses,
            skip_sender_addresses,
            focus_user_transactions,
        }
    }

    /// Returns true if the transaction should be included
    pub fn include(&self, transaction: &Transaction) -> bool {
        // If we're only focusing on user transactions, skip if it's not a user transaction

        let is_user_txn = transaction.r#type == TransactionType::User as i32;
        if self.focus_user_transactions && !is_user_txn {
            return false;
        }

        // If it's not a user transaction, we can skip the rest of the checks
        if !is_user_txn {
            return true;
        }

        if let Some(TxnData::User(user_transaction)) = transaction.txn_data.as_ref() {
            if let Some(utr) = user_transaction.request.as_ref() {
                // Skip if sender is in the skip list
                if let Some(skip_sender_addresses) = &self.skip_sender_addresses {
                    if skip_sender_addresses.contains(&utr.sender) {
                        return false;
                    }
                }

                if let Some(focus_contract_addresses) = &self.focus_contract_addresses {
                    // Skip if focus contract addresses are set and the transaction isn't in the list
                    if let Some(payload) = utr.payload.as_ref() {
                        if let Some(Payload::EntryFunctionPayload(efp)) = payload.payload.as_ref() {
                            if let Some(function) = efp.function.as_ref() {
                                if let Some(module) = function.module.as_ref() {
                                    if !focus_contract_addresses.contains(&module.address) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        true
    }
}
