// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{BurnRoomEvent, CreateRoomEvent, Room};
use crate::{
    models::token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
    schema::tournament_rooms,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use aptos_protos::transaction::v1::{Event, WriteResource};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type RoomAddress = String;
pub type TournamentRoomMapping = HashMap<RoomAddress, TournamentRoom>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournament_rooms)]
pub struct TournamentRoom {
    pub address: String,
    pub tournament_address: String,
    pub round_address: String,
    pub in_progress: bool,
    pub last_transaction_version: i64,
}

impl TournamentRoom {
    pub fn pk(&self) -> String {
        self.address.clone()
    }

    pub fn from_write_resource(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
        create_room_events: &HashMap<String, CreateRoomEvent>,
    ) -> Option<Self> {
        if Room::from_write_resource(contract_addr, write_resource, transaction_version)
            .unwrap()
            .is_some()
        {
            let address = standardize_address(&write_resource.address);
            let create_room_event = create_room_events.get(&address).unwrap();
            return Some(TournamentRoom {
                address: address.clone(),
                tournament_address: create_room_event.get_tournament_address(),
                round_address: create_room_event.get_current_round_address(),
                in_progress: true,
                last_transaction_version: transaction_version,
            });
        }
        None
    }

    pub async fn delete_room(
        conn: &mut PgPoolConnection<'_>,
        contract_addr: &str,
        event: &Event,
        transaction_version: i64,
        previous_tournament_rooms: &TournamentRoomMapping,
    ) -> Option<Self> {
        if let Some(burn_player_token_event) =
            BurnRoomEvent::from_event(contract_addr, event, transaction_version).unwrap()
        {
            let object_address = burn_player_token_event.get_object_address();
            match previous_tournament_rooms.get(&object_address) {
                Some(room) => {
                    return Some(TournamentRoom {
                        address: object_address.to_string(),
                        tournament_address: room.tournament_address.clone(),
                        round_address: room.round_address.clone(),
                        in_progress: false,
                        last_transaction_version: transaction_version,
                    });
                },
                None => {
                    if let Some(room) =
                        TournamentRoomQuery::query_by_address(conn, &object_address).await
                    {
                        return Some(TournamentRoom {
                            address: room.address.clone(),
                            tournament_address: room.tournament_address.clone(),
                            round_address: room.round_address.clone(),
                            in_progress: false,
                            last_transaction_version: transaction_version,
                        });
                    }
                },
            }
        }
        None
    }
}

impl Ord for TournamentRoom {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.address.cmp(&other.address)
    }
}

impl PartialOrd for TournamentRoom {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Queryable, Identifiable, Debug, Clone)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournament_rooms)]
pub struct TournamentRoomQuery {
    pub address: String,
    pub tournament_address: String,
    pub round_address: String,
    pub in_progress: bool,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
}

impl TournamentRoomQuery {
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
        tournament_rooms::table
            .find(address)
            .first::<Self>(conn)
            .await
            .optional()
    }
}
