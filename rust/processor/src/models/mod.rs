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
        && events[index - 1]
            .type_str
            .starts_with(&event.type_str[..len - 5])
}

#[cfg(test)]
mod tests {
    use super::*;
    // Tests are to make sure
    // - only when previous processed event is event v2, current event v1 can be skipped

    #[test]
    fn test_should_skip_intended() {
        let event1 = Event {
            type_str: "0x1::coin::Deposit<0x1::aptos_coin::AptosCoin>".to_string(),
            ..Event::default()
        };
        let event2 = Event {
            type_str: "0x1::coin::DepositEvent".to_string(),
            ..Event::default()
        };
        let events = vec![event1, event2];
        assert!(!should_skip(0, &events[0], &events));
        assert!(should_skip(1, &events[1], &events));
    }

    #[test]
    fn test_should_not_break_for_length() {
        let events = [
            Event {
                type_str: "Test0000000000000000000000000000000Event".to_string(),
                ..Event::default()
            },
            Event {
                type_str: "TEvent".to_string(),
                ..Event::default()
            },
            Event {
                type_str: "Test0000000000000000000000000000000Event".to_string(),
                ..Event::default()
            },
        ];
        assert!(!should_skip(0, &events[0], &events));
        // Note, it is intentional that the second event is skipped.
        assert!(should_skip(1, &events[1], &events));
        assert!(!should_skip(2, &events[2], &events));
    }
}
