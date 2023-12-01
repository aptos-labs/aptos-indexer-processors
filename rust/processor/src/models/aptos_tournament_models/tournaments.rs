// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{CurrentRound, TournamentDirector, TournamentState};
use crate::{
    models::token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
    schema::tournaments,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use aptos_protos::transaction::v1::WriteResource;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;

pub type TournamentMapping = HashMap<String, Tournament>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, PartialEq, Eq,
)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournaments)]
pub struct Tournament {
    pub address: String,
    pub tournament_name: String,
    pub max_players: i64,
    pub max_num_winners: i64,
    pub players_joined: i64,
    pub is_joinable: bool,
    pub has_ended: bool,
    pub current_round_address: Option<String>,
    pub current_round_number: i64,
    pub current_game_module: Option<String>,
    pub last_transaction_version: i64,
}

impl Tournament {
    pub fn from_write_resource(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
        tournament_state_mapping: HashMap<String, TournamentState>,
        current_round_mapping: HashMap<String, CurrentRound>,
    ) -> Option<Self> {
        if let Some(td) = TournamentDirector::from_write_resource(
            contract_addr,
            write_resource,
            transaction_version,
        )
        .unwrap()
        {
            let tournament_address = standardize_address(&write_resource.address);
            let state = tournament_state_mapping
                .get(&tournament_address)
                .unwrap_or_else(|| {
                    error!(
                        transaction_version = transaction_version,
                        tournament_state_mapping = ?tournament_state_mapping,
                        tournament_address = tournament_address,
                        "Can't find tournament state mapping"
                    );
                    panic!("Can't find tournament state mapping");
                });

            let current_round = current_round_mapping
                .get(&tournament_address)
                .unwrap_or_else(|| {
                    error!(
                        transaction_version = transaction_version,
                        current_round_mapping = ?current_round_mapping,
                        tournament_address = tournament_address,
                        "Can't find current round mapping"
                    );
                    panic!("Can't find current round mapping");
                });

            return Some(Tournament {
                address: tournament_address,
                tournament_name: td.tournament_name,
                max_players: td.max_players,
                max_num_winners: td.max_num_winners,
                players_joined: td.players_joined,
                is_joinable: state.is_joinable,
                has_ended: state.has_ended,
                current_round_address: Some(current_round.get_round_address()),
                current_round_number: current_round.number,
                current_game_module: Some(current_round.game_module.clone()),
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for Tournament {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.address.cmp(&other.address)
    }
}

impl PartialOrd for Tournament {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Queryable, Identifiable, Debug, Clone)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournaments)]
pub struct TournamentQuery {
    pub address: String,
    pub tournament_name: String,
    pub max_players: i64,
    pub max_num_winners: i64,
    pub players_joined: i64,
    pub is_joinable: bool,
    pub has_ended: bool,
    pub current_round_address: Option<String>,
    pub current_round_number: i64,
    pub current_game_module: Option<String>,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
}

impl TournamentQuery {
    pub async fn query_by_address(conn: &mut PgPoolConnection<'_>, address: &str) -> Option<Self> {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            if let Ok(player) = Self::get_by_address(conn, address).await {
                return player;
            }
            std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
        }
        None
    }

    async fn get_by_address(
        conn: &mut PgPoolConnection<'_>,
        address: &str,
    ) -> Result<Option<Self>, diesel::result::Error> {
        tournaments::table
            .find(address)
            .first::<Self>(conn)
            .await
            .optional()
    }
}
