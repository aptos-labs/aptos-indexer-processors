// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

include!(concat!(env!("OUT_DIR"), "/generate_transactions.rs"));

// TODO: Add some orderless transactions here for testing.
// Question: Should we do it after the orderless tranactions feature is deployed?
#[cfg(test)]
mod tests {
    use super::*;
    use aptos_protos::transaction::v1::Transaction;

    #[test]
    fn test_generate_transactions() {
        let json_bytes = SCRIPTED_TRANSACTIONS_SIMPLE_USER_SCRIPT1;
        // Check that the transaction is valid JSON
        let transaction = serde_json::from_slice::<Transaction>(json_bytes).unwrap();

        assert_eq!(transaction.version, 61);
    }
}
