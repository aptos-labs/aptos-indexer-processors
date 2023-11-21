// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{Room, TournamentToken};
use crate::{schema::tournament_players, utils::util::standardize_address};
use aptos_protos::transaction::v1::WriteResource;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type TokenAddress = String;
pub type TournamentPlayerMapping = HashMap<TokenAddress, TournamentPlayer>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(token_address))]
#[diesel(table_name = tournament_players)]
pub struct TournamentPlayer {
    pub token_address: String,
    pub user_address: String,
    pub tournament_address: String,
    pub room_address: Option<String>,
    pub alive: bool,
    pub last_transaction_version: i64,
}

impl TournamentPlayer {
    pub fn pk(&self) -> String {
        self.token_address.clone()
    }

    pub fn from_tournament_token(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
        token_to_owner: &HashMap<String, String>,
        previous_tournament_token: &TournamentPlayerMapping,
    ) -> Option<Self> {
        if let Some(tournament_token) =
            TournamentToken::from_write_resource(contract_addr, write_resource, transaction_version)
                .unwrap()
        {
            let token_address = standardize_address(&write_resource.address);
            let room_address = match previous_tournament_token.get(&token_address) {
                Some(player) => player.room_address.clone(),
                None => {
                    // look it up in the db. If can't find, then do nothing
                    None
                },
            };
            return Some(TournamentPlayer {
                token_address: token_address.clone(),
                user_address: token_to_owner.get(&token_address).unwrap().clone(),
                tournament_address: tournament_token.get_tournament_address(),
                room_address,
                alive: true,
                last_transaction_version: transaction_version,
            });
        }
        None
    }

    pub fn from_room(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
        previous_tournament_token: &TournamentPlayerMapping,
    ) -> TournamentPlayerMapping {
        let mut players = HashMap::new();
        if let Some(room) =
            Room::from_write_resource(contract_addr, write_resource, transaction_version).unwrap()
        {
            let room_address = &standardize_address(&write_resource.address);
            for player in room.get_players().iter() {
                let token_address = standardize_address(&player);
                let room_player = Self::from_room_player(
                    &token_address,
                    transaction_version,
                    room_address,
                    previous_tournament_token,
                );
                players.insert(room_player.token_address.clone(), room_player);
            }
        }
        players
    }

    fn from_room_player(
        token_address: &str,
        transaction_version: i64,
        room_address: &str,
        previous_tournament_token: &TournamentPlayerMapping,
    ) -> Self {
        match previous_tournament_token.get(token_address) {
            Some(player) => TournamentPlayer {
                token_address: token_address.to_string(),
                user_address: player.user_address.clone(),
                tournament_address: player.tournament_address.clone(),
                room_address: Some(room_address.to_string()),
                alive: true,
                last_transaction_version: transaction_version,
            },
            None => {
                // look it up in the db. If can't find, then do crash
                panic!("Tournament token must be present")
            },
        }
    }
}

impl Ord for TournamentPlayer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_address.cmp(&other.token_address)
    }
}

impl PartialOrd for TournamentPlayer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
