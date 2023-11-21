// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{Room, TournamentToken};
use crate::{schema::tournament_players, utils::database::PgPoolConnection};
use aptos_protos::transaction::v1::WriteResource;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;

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

#[derive(Clone, Debug, Deserialize, Identifiable, Queryable, Serialize)]
#[diesel(primary_key(token_address))]
#[diesel(table_name = tournament_players)]
pub struct TournamentPlayerQuery {
    pub token_address: String,
    pub user_address: String,
    pub tournament_address: String,
    pub room_address: Option<String>,
    pub alive: bool,
    pub submitted: bool,
    pub inserted_at: chrono::NaiveDateTime,
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

    pub fn from_write_resource_room(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
    ) -> Vec<Self> {
        let mut players = Vec::new();
        if let Some(room) =
            Room::from_write_resource(contract_addr, write_resource, transaction_version).unwrap()
        {
            for player in room.players.vec[0].iter() {
                players.push(TournamentPlayer {
                    token_address: player.inner.clone(),
                    user_address: "".to_string(),
                    tournament_address: "".to_string(),
                    room_address: Some(write_resource.address.clone()),
                    alive: true,
                    submitted: false,
                });
            }
        }
        players
    }
}

impl TournamentPlayerQuery {
    pub async fn query(conn: &mut PgPoolConnection<'_>, token_address: &str) -> Option<Self> {
        tournament_players::table
            .find(token_address)
            .first::<TournamentPlayerQuery>(conn)
            .await
            .optional()
            .unwrap_or_else(|e| {
                error!(
                    token_address = token_address,
                    error = ?e,
                    "Error querying tournament player"
                );
                panic!();
            })
    }
}
