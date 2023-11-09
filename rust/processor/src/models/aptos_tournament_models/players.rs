// Copyright © Aptos Foundation

use crate::aptos_tournament_schema::aptos_tournament::players;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(address, tournament_address, round))]
#[diesel(table_name = players)]
pub struct Players {
    address: String,
    tournament_address: String,
    round: i32,
    alive: bool,
    submitted: bool,
}

impl Players {
    pub fn new(address: String, tournament_address: String, round: i32) -> Self {
        Self {
            address,
            tournament_address,
            round,
            alive: true,
            submitted: false,
        }
    }
}
