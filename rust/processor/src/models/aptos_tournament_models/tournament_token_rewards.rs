// Copyright © Aptos Foundation

use crate::{
    models::{
        aptos_tournament_models::aptos_tournament_utils::{AddTokenV1Reward, RemoveTokenV1Reward},
        token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
    },
    schema::tournament_token_rewards,
    utils::database::PgPoolConnection,
};
use aptos_protos::transaction::v1::Event;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TournamentTokenRewardMapping = HashMap<String, TournamentTokenReward>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(tournament_address))]
#[diesel(table_name = tournament_token_rewards)]
pub struct TournamentTokenReward {
    pub tournament_address: String,
    pub tokens: Vec<Option<String>>,
    pub last_transaction_version: i64,
}

impl TournamentTokenReward {
    pub fn pk(&self) -> String {
        self.tournament_address.clone()
    }

    async fn lookup(
        conn: &mut PgPoolConnection<'_>,
        tournament_address: &str,
        previous_tournament_token_rewards: &TournamentTokenRewardMapping,
    ) -> Option<Self> {
        match previous_tournament_token_rewards.get(tournament_address) {
            Some(rewards) => Some(rewards.clone()),
            None => {
                TournamentTokenRewardQuery::query_by_tournament_address(conn, tournament_address)
                    .await
                    .map(Into::into)
            },
        }
    }

    pub async fn from_event(
        conn: &mut PgPoolConnection<'_>,
        contract_addr: &str,
        event: &Event,
        transaction_version: i64,
        previous_tournament_token_rewards: &TournamentTokenRewardMapping,
    ) -> Option<Self> {
        let mut rewards = None;
        if let Some(add) =
            AddTokenV1Reward::from_event(contract_addr, event, transaction_version).unwrap()
        {
            rewards = Some({
                let mut prev = Self::lookup(
                    conn,
                    &add.get_tournament_address(),
                    previous_tournament_token_rewards,
                )
                .await
                .unwrap_or(Self {
                    tournament_address: add.get_tournament_address(),
                    tokens: vec![],
                    last_transaction_version: transaction_version,
                });
                prev.tokens.push(Some(add.get_token_data_id()));
                prev
            });
        };
        if let Some(remove) =
            RemoveTokenV1Reward::from_event(contract_addr, event, transaction_version).unwrap()
        {
            if rewards.is_none() {
                rewards = Some(
                    Self::lookup(
                        conn,
                        &remove.get_tournament_address(),
                        previous_tournament_token_rewards,
                    )
                    .await
                    .unwrap(),
                );
            }

            if let Some(mut r) = rewards.clone() {
                if let Some(pos) = r
                    .tokens
                    .iter()
                    .position(|t| t.clone().unwrap() == remove.get_token_data_id())
                {
                    r.tokens.remove(pos);
                    rewards = Some(r)
                }
            }
        };
        rewards
    }
}

impl Ord for TournamentTokenReward {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tournament_address.cmp(&other.tournament_address)
    }
}

impl PartialOrd for TournamentTokenReward {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Queryable, Identifiable, Debug, Clone)]
#[diesel(primary_key(tournament_address))]
#[diesel(table_name = tournament_token_rewards)]
pub struct TournamentTokenRewardQuery {
    pub tournament_address: String,
    pub tokens: Vec<Option<String>>,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
}

impl TournamentTokenRewardQuery {
    pub async fn query_by_tournament_address(
        conn: &mut PgPoolConnection<'_>,
        tournament_address: &str,
    ) -> Option<Self> {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            if let Ok(player) = Self::get_by_tournament_address(conn, tournament_address).await {
                return player;
            }
            std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
        }
        None
    }

    async fn get_by_tournament_address(
        conn: &mut PgPoolConnection<'_>,
        tournament_address: &str,
    ) -> Result<Option<Self>, diesel::result::Error> {
        tournament_token_rewards::table
            .find(tournament_address)
            .first::<Self>(conn)
            .await
            .optional()
    }
}

impl From<TournamentTokenRewardQuery> for TournamentTokenReward {
    fn from(query: TournamentTokenRewardQuery) -> Self {
        Self {
            tournament_address: query.tournament_address,
            tokens: query.tokens,
            last_transaction_version: query.last_transaction_version,
        }
    }
}
