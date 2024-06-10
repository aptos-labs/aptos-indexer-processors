// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{schema::current_pokes, utils::util::standardize_address};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::Event;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// TODO: Surprising that we have to write these, I'd assume it'd be generated for us.
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(initial_poker_address, recipient_poker_address))]
#[diesel(table_name = current_pokes)]
pub struct Poke {
    /// The person who initiated the poke pair.
    pub initial_poker_address: String,
    /// The other side of the poke pair.
    pub recipient_poker_address: String,
    /// True if the initial poker is the next poker.
    pub initial_poker_is_next_poker: bool,
    pub times_poked: i64,
    pub last_transaction_version: i64,
    pub last_poke_at: chrono::NaiveDateTime,
}

impl Poke {
    /// Consume a raw event and maybe produce a Poke. If the event was the wrong
    /// type, we return None.
    pub fn from_event(
        event: &Event,
        module_address: &str,
        last_transaction_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> Result<Option<Self>> {
        if event.type_str != format!("{}::poke::PokeEvent", module_address) {
            return Ok(None);
        }
        let data: EventData =
            serde_json::from_str(&event.data).context("Failed to parse PokeEvent")?;
        let times_poked: i64 = data
            .times_poked
            .parse()
            .context("Failed to parse times_poked")?;
        let from_address = standardize_address(&data.from);
        let to_address = standardize_address(&data.to);
        let (initial_poker_address, recipient_poker_address, initial_poker_is_next_poker) =
            if times_poked % 2 != 0 {
                (from_address, to_address, false)
            } else {
                (to_address, from_address, true)
            };
        let poke = Poke {
            initial_poker_address,
            recipient_poker_address,
            initial_poker_is_next_poker,
            times_poked,
            last_transaction_version,
            last_poke_at: txn_timestamp,
        };
        println!("Based on event {:?} inserting {:?}", event, poke);
        Ok(Some(poke))
    }
}

/// This is a helper for reading the JSON of the event. We only consume the fields we
/// care about (e.g. we ignore global_pokes).
#[derive(Debug, Deserialize, Serialize)]
struct EventData {
    pub from: String,
    pub to: String,
    pub times_poked: String,
}
