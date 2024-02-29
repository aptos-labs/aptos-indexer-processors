// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{BurnRoomEvent, CreateRoomEvent, Room};
use crate::{schema::tournament_rooms, utils::util::standardize_address};
use aptos_protos::transaction::v1::{Event, WriteResource};
use diesel::prelude::*;
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
            if let Some(create_room_event) = create_room_events.get(&address) {
                return Some(TournamentRoom {
                    address: address.clone(),
                    tournament_address: create_room_event.get_tournament_address(),
                    round_address: create_room_event.get_current_round_address(),
                    in_progress: true,
                    last_transaction_version: transaction_version,
                });
            }
        }
        None
    }

    pub fn delete_room(
        contract_addr: &str,
        event: &Event,
        transaction_version: i64,
    ) -> Option<Self> {
        if let Some(burn_player_token_event) =
            BurnRoomEvent::from_event(contract_addr, event, transaction_version).unwrap()
        {
            let object_address = burn_player_token_event.get_object_address();
            return Some(TournamentRoom {
                address: object_address,
                tournament_address: String::new(),
                round_address: String::new(),
                in_progress: false,
                last_transaction_version: transaction_version,
            });
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
