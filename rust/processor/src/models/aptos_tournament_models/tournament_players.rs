// Copyright © Aptos Foundation

use super::aptos_tournament_utils::TournamentToken;
use crate::schema::tournament_players;
use aptos_protos::transaction::v1::WriteResource;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_address))]
#[diesel(table_name = tournament_players)]
pub struct TournamentPlayer {
    pub token_address: String,
    pub user_address: String,
    pub tournament_address: String,
    pub room_address: Option<String>,
    pub alive: bool,
    pub submitted: bool,
}

impl TournamentPlayer {
    pub fn from_write_resource(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
        token_to_owner: HashMap<String, (String, String)>,
    ) -> Option<Self> {
        if let Some(tournament_token) =
            TournamentToken::from_write_resource(contract_addr, write_resource, transaction_version)
                .unwrap()
        {
            Some(TournamentPlayer {
                token_address: write_resource.address.clone(),
                user_address: token_to_owner
                    .get(&write_resource.address)
                    .unwrap()
                    .0
                    .to_string(),
                tournament_address: tournament_token.get_tournament_address(),
                room_address: Some(tournament_token.get_room_address()),
                alive: true,
                submitted: false,
            });
        }
        None
    }
}
