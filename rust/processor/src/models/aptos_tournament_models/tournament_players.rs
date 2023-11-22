// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{Room, TournamentToken};
use crate::{
    models::{
        token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
        token_v2_models::v2_token_utils::TokenV2Burned,
    },
    schema::tournament_players,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;

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

    pub async fn from_tournament_token(
        conn: &mut PgPoolConnection<'_>,
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
                None => match TournamentPlayerQuery::query_by_token_address(conn, &token_address)
                    .await
                {
                    Some(player) => player.room_address,
                    None => None,
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

    pub async fn from_room(
        conn: &mut PgPoolConnection<'_>,
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
                    conn,
                    &token_address,
                    transaction_version,
                    room_address,
                    previous_tournament_token,
                )
                .await;
                players.insert(room_player.token_address.clone(), room_player);
            }
        }
        players
    }

    async fn from_room_player(
        conn: &mut PgPoolConnection<'_>,
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
                let player = TournamentPlayerQuery::query_by_token_address(conn, token_address)
                    .await
                    .unwrap_or_else(|| {
                        error!("Tournament player not found in database");
                        panic!();
                    });

                TournamentPlayer {
                    token_address: player.token_address,
                    user_address: player.user_address,
                    tournament_address: player.tournament_address,
                    room_address: Some(room_address.to_string()),
                    alive: true,
                    last_transaction_version: transaction_version,
                }
            },
        }
    }

    pub async fn delete_player(
        conn: &mut PgPoolConnection<'_>,
        delete_resource: &DeleteResource,
        transaction_version: i64,
        tokens_burned: &TokenV2Burned,
        previous_tournament_token: &TournamentPlayerMapping,
    ) -> Option<Self> {
        if let Some(token_address) =
            tokens_burned.get(&standardize_address(&delete_resource.address.to_string()))
        {
            match previous_tournament_token.get(token_address) {
                Some(player) => {
                    return Some(TournamentPlayer {
                        token_address: token_address.to_string(),
                        user_address: player.user_address.clone(),
                        tournament_address: player.tournament_address.clone(),
                        room_address: player.room_address.clone(),
                        alive: false,
                        last_transaction_version: transaction_version,
                    });
                },
                None => {
                    if let Some(player) =
                        TournamentPlayerQuery::query_by_token_address(conn, token_address).await
                    {
                        return Some(TournamentPlayer {
                            token_address: player.token_address,
                            user_address: player.user_address,
                            tournament_address: player.tournament_address,
                            room_address: player.room_address,
                            alive: false,
                            last_transaction_version: transaction_version,
                        });
                    }
                },
            }
        }
        None
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

#[derive(Queryable, Identifiable, Debug, Clone)]
#[diesel(primary_key(token_address))]
#[diesel(table_name = tournament_players)]
pub struct TournamentPlayerQuery {
    pub token_address: String,
    pub user_address: String,
    pub tournament_address: String,
    pub room_address: Option<String>,
    pub alive: bool,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
}

impl TournamentPlayerQuery {
    pub async fn query_by_token_address(
        conn: &mut PgPoolConnection<'_>,
        token_address: &str,
    ) -> Option<Self> {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            if let Some(player) = Self::get_by_token_address(conn, token_address).await.ok() {
                return player;
            }
            std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
        }
        None
    }

    async fn get_by_token_address(
        conn: &mut PgPoolConnection<'_>,
        token_address: &str,
    ) -> Result<Option<Self>, diesel::result::Error> {
        tournament_players::table
            .find(token_address)
            .first::<Self>(conn)
            .await
            .optional()
    }
}
