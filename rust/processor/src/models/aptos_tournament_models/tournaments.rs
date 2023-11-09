// Copyright © Aptos Foundation

use crate::aptos_tournament_schema::aptos_tournament::tournaments;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournaments)]
pub struct Tournaments {
    address: String,
    tournament_name: String,
    max_players: i32,
    num_winners: i32,
    current_round: i32,
    has_started: bool,
    is_joinable: bool,
}

impl Tournaments {
    pub fn new(
        address: String,
        tournament_name: String,
        max_players: i32,
        num_winners: i32,
    ) -> Self {
        Self {
            address,
            tournament_name,
            max_players,
            num_winners,
            current_round: 0,
            has_started: false,
            is_joinable: true,
        }
    }
}
