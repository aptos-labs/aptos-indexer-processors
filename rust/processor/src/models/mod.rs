// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_protos::transaction::v1::Event;

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

pub(crate) fn should_skip(index: usize, event: &Event, events: &[Event]) -> bool {
    let len = event.type_str.len();
    index > 0
        && event.type_str.ends_with("Event")
        && events[index - 1].type_str[..len - 5] == event.type_str[..len - 5]
}
