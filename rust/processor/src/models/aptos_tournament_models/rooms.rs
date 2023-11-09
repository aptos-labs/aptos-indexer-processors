// Copyright © Aptos Foundation

use crate::aptos_tournament_schema::aptos_tournament::rooms;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(tournament_address, round))]
#[diesel(belongs_to(Tournament, foreign_key = tournament_address))]
#[diesel(table_name = rooms)]
pub struct Rooms {
    tournament_address: String,
    round: i32,
    address: String,
    players_per_room: Option<i32>,
}

impl Rooms {
    pub fn new(
        address: String,
        tournament_address: String,
        round: i32,
        players_per_room: Option<i32>,
    ) -> Self {
        Self {
            address,
            tournament_address,
            round,
            players_per_room,
        }
    }
}
