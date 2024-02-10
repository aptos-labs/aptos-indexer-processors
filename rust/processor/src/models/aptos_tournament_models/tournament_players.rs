// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{
    BurnPlayerTokenEvent, BurnRoomEvent, CoinRewardClaimed, Room, TournamentPlayerToken,
};
use crate::{
    models::token_models::{
        collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
        token_utils::TokenEvent,
    },
    schema::tournament_players,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use aptos_protos::transaction::v1::{Event, WriteResource};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
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
    pub player_name: String,
    pub alive: bool,
    pub token_uri: String,
    pub coin_reward_claimed_type: Option<String>,
    pub coin_reward_claimed_amount: Option<i64>,
    pub token_reward_claimed: Vec<Option<String>>,
    pub last_transaction_version: i64,
}

impl TournamentPlayer {
    pub fn pk(&self) -> String {
        self.token_address.clone()
    }

    async fn lookup(
        conn: &mut PgPoolConnection<'_>,
        token_address: &str,
        previous_tournament_token: &TournamentPlayerMapping,
    ) -> Self {
        match previous_tournament_token.get(token_address) {
            Some(player) => player.clone(),
            None => {
                let player = TournamentPlayerQuery::query_by_token_address(conn, token_address)
                    .await
                    .unwrap();

                TournamentPlayer {
                    token_address: player.token_address,
                    user_address: player.user_address,
                    tournament_address: player.tournament_address,
                    room_address: player.room_address,
                    player_name: player.player_name,
                    alive: player.alive,
                    token_uri: player.token_uri.clone(),
                    coin_reward_claimed_type: player.coin_reward_claimed_type.clone(),
                    coin_reward_claimed_amount: player.coin_reward_claimed_amount,
                    token_reward_claimed: player.token_reward_claimed.clone(),
                    last_transaction_version: player.last_transaction_version,
                }
            },
        }
    }

    pub fn from_tournament_token(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
        object_to_owner: &HashMap<String, String>,
    ) -> Option<Self> {
        if let Some(tournament_token) = TournamentPlayerToken::from_write_resource(
            contract_addr,
            write_resource,
            transaction_version,
        )
        .unwrap()
        {
            let token_address = standardize_address(&write_resource.address);
            return Some(TournamentPlayer {
                token_address: token_address.clone(),
                user_address: object_to_owner.get(&token_address).unwrap().clone(),
                tournament_address: tournament_token.get_tournament_address(),
                room_address: None,
                player_name: tournament_token.player_name.clone(),
                alive: true,
                token_uri: tournament_token.token_uri,
                coin_reward_claimed_type: None,
                coin_reward_claimed_amount: None,
                token_reward_claimed: vec![],
                last_transaction_version: transaction_version,
            });
        }
        None
    }

    pub fn from_room(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
    ) -> TournamentPlayerMapping {
        let mut players = HashMap::new();
        if let Some(room) =
            Room::from_write_resource(contract_addr, write_resource, transaction_version).unwrap()
        {
            let room_address = &standardize_address(&write_resource.address);
            for player in room.get_players().iter() {
                let token_address = standardize_address(player);
                players.insert(
                    token_address.clone(),
                    TournamentPlayer {
                        token_address,
                        user_address: String::new(),
                        tournament_address: String::new(),
                        room_address: Some(room_address.to_string()),
                        player_name: String::new(),
                        alive: true,
                        token_uri: String::new(),
                        coin_reward_claimed_type: None,
                        coin_reward_claimed_amount: None,
                        token_reward_claimed: vec![],
                        last_transaction_version: transaction_version,
                    },
                );
            }
        }
        players
    }

    pub fn delete_player(
        contract_addr: &str,
        event: &Event,
        transaction_version: i64,
    ) -> Option<Self> {
        if let Some(burn_player_token_event) =
            BurnPlayerTokenEvent::from_event(contract_addr, event, transaction_version).unwrap()
        {
            let object_address = burn_player_token_event.get_object_address();
            return Some(TournamentPlayer {
                token_address: object_address.to_string(),
                user_address: String::new(),
                tournament_address: String::new(),
                room_address: None,
                player_name: String::new(),
                alive: false,
                token_uri: String::new(),
                coin_reward_claimed_type: None,
                coin_reward_claimed_amount: None,
                token_reward_claimed: vec![],
                last_transaction_version: transaction_version,
            });
        }
        None
    }

    pub fn delete_room(
        contract_addr: &str,
        event: &Event,
        transaction_version: i64,
    ) -> TournamentPlayerMapping {
        let mut players = HashMap::new();
        if let Some(burn_room_event) =
            BurnRoomEvent::from_event(contract_addr, event, transaction_version).unwrap()
        {
            for player_addr in burn_room_event.get_players() {
                let player = TournamentPlayer {
                    token_address: player_addr,
                    user_address: String::new(),
                    tournament_address: String::new(),
                    room_address: None,
                    player_name: String::new(),
                    alive: true,
                    token_uri: String::new(),
                    coin_reward_claimed_type: None,
                    coin_reward_claimed_amount: None,
                    token_reward_claimed: vec![],
                    last_transaction_version: transaction_version,
                };
                players.insert(player.token_address.clone(), player);
            }
        }
        players
    }

    pub fn claim_coin_reward(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
    ) -> Option<Self> {
        if let Some(coin_reward) = CoinRewardClaimed::from_write_resource(
            contract_addr,
            write_resource,
            transaction_version,
        )
        .unwrap()
        {
            let object_address = standardize_address(&write_resource.address);
            let type_ = write_resource.type_str.clone();
            let type_arg = &type_[type_.find('<').unwrap() + 1..type_.find('>').unwrap()];
            return Some(TournamentPlayer {
                token_address: object_address,
                user_address: String::new(),
                tournament_address: String::new(),
                room_address: None,
                player_name: String::new(),
                alive: true,
                token_uri: String::new(),
                coin_reward_claimed_type: Some(type_arg.to_string()),
                coin_reward_claimed_amount: Some(coin_reward.amount),
                token_reward_claimed: vec![],
                last_transaction_version: transaction_version,
            });
        }
        None
    }

    pub async fn claim_token_reward(
        conn: &mut PgPoolConnection<'_>,
        event: &Event,
        transaction_version: i64,
        receiver_to_object: &HashMap<String, String>,
        previous_tournament_token: &TournamentPlayerMapping,
    ) -> Option<Self> {
        if let Some(TokenEvent::DepositTokenEvent(deposit_event)) = TokenEvent::from_event(
            event.type_str.as_str(),
            event.data.as_str(),
            transaction_version,
        )
        .unwrap()
        {
            let recv_addr = standardize_address(&event.clone().key.unwrap().account_address);
            if let Some(object_address) = receiver_to_object.get(&recv_addr) {
                let token_hash = deposit_event.id.token_data_id.to_hash();
                let mut player =
                    Self::lookup(conn, object_address, previous_tournament_token).await;
                player.token_reward_claimed.push(Some(token_hash));
                player.last_transaction_version = transaction_version;
                return Some(player);
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
    pub player_name: String,
    pub alive: bool,
    pub token_uri: String,
    pub coin_reward_claimed_type: Option<String>,
    pub coin_reward_claimed_amount: Option<i64>,
    pub token_reward_claimed: Vec<Option<String>>,
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
            if let Ok(player) = Self::get_by_token_address(conn, token_address).await {
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

    pub async fn query_by_room_address(
        conn: &mut PgPoolConnection<'_>,
        room_address: &str,
    ) -> Vec<Self> {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            if let Ok(players) = Self::get_by_room_address(conn, room_address).await {
                return players;
            }
            std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
        }
        vec![]
    }

    async fn get_by_room_address(
        conn: &mut PgPoolConnection<'_>,
        room_address: &str,
    ) -> Result<Vec<Self>, diesel::result::Error> {
        tournament_players::table
            .filter(tournament_players::room_address.eq(room_address))
            .load::<Self>(conn)
            .await
    }
}
