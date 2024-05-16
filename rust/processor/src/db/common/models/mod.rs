// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_protos::transaction::v1::Event;
use lazy_static::lazy_static;
use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
};

pub mod account_transaction_models;
pub mod ans_models;
pub mod coin_models;
pub mod default_models;
pub mod events_models;
pub mod fungible_asset_models;
pub mod ledger_info;
pub mod object_models;
pub mod processor_status;
pub mod property_map;
pub mod stake_models;
pub mod token_models;
pub mod token_v2_models;
pub mod transaction_metadata_model;
pub mod user_transactions_models;

static EVENT_V2_ENABLED: AtomicBool = AtomicBool::new(false);

lazy_static! {
    pub static ref EVENT_V2_SET: HashSet<&'static str> = {
        vec![
            "0x1::aptos_governance::Vote",
            "0x1::coin::CoinDeposit",
            "0x1::coin::CoinWithdraw",
            "0x1::delegation_pool::AddStake",
            "0x1::delegation_pool::UnlockStake",
            "0x1::delegation_pool::WithdrawStake",
            "0x1::delegation_pool::ReactivateStake",
            "0x1::object::Transfer",
            "0x1::stake::DistributeRewards",
            "0x3::token::MintToken",
            "0x3::token::BurnToken",
            "0x3::token::MutateTokenPropertyMap",
            "0x3::token::Withdraw",
            "0x3::token::Deposit",
            "0x3::token_transfers::TokenOffer",
            "0x3::token_transfers::TokenCancelOffer",
            "0x3::token_transfers::TokenClaim",
            "0x4::token::Mutation",
        ]
        .into_iter()
        .collect()
    };
}

pub(crate) fn should_skip(event: &Event) -> bool {
    if EVENT_V2_SET.contains(event.type_str.as_str()) {
        let _ =
            EVENT_V2_ENABLED.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed);
        false
    } else {
        EVENT_V2_ENABLED.load(Ordering::Relaxed)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    // Tests are to make sure
    // - only when previous processed event is event v2, current event v1 can be skipped
    #[test]
    fn test_should_not_break_for_length() {
        let events = [
            Event {
                type_str: "0x4::token::Mutation".to_string(),
                ..Event::default()
            },
            Event {
                type_str: "0x4::token::MutationEvent".to_string(),
                ..Event::default()
            },
        ];
        assert!(!should_skip(&events[0]));
        assert!(should_skip(&events[1]));
    }
}
