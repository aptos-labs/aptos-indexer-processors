// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{AptosTournamentResource, RPSResultEvent};
use crate::{
    models::default_models::move_resources::MoveResource, schema::rock_paper_scissors_games,
};
use aptos_protos::transaction::v1::{Event, WriteResource};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type RockPaperScissorsGameMapping = HashMap<String, RockPaperScissorsGame>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(room_address))]
#[diesel(table_name = rock_paper_scissors_games)]
pub struct RockPaperScissorsGame {
    pub room_address: String,
    pub player1_token_address: String,
    pub player2_token_address: String,
    pub last_transaction_version: i64,
    pub winners: Option<Vec<Option<String>>>,
    pub losers: Option<Vec<Option<String>>>,
}

impl RockPaperScissorsGame {
    pub fn pk(&self) -> String {
        self.room_address.clone()
    }

    pub fn from_write_resource(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
    ) -> Option<Self> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(contract_addr, type_str.as_str()) {
            return None;
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            transaction_version,
            0, // Placeholder, this isn't used anyway
        );

        if let AptosTournamentResource::RockPaperScissors(inner) =
            AptosTournamentResource::from_resource(
                contract_addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                transaction_version,
            )
            .unwrap()
        {
            return Some(RockPaperScissorsGame {
                room_address: resource.address.clone(),
                player1_token_address: inner.player1.get_token_address(),
                player2_token_address: inner.player2.get_token_address(),
                last_transaction_version: transaction_version,
                winners: None,
                losers: None,
            });
        }
        None
    }

    pub fn from_results(
        contract_addr: &str,
        event: &Event,
        transaction_version: i64,
    ) -> Option<Self> {
        if let Some(results) =
            RPSResultEvent::from_event(contract_addr, event, transaction_version).unwrap()
        {
            return Some(RockPaperScissorsGame {
                room_address: results.get_game_address(),
                player1_token_address: "".to_string(),
                player2_token_address: "".to_string(),
                last_transaction_version: transaction_version,
                winners: Some(results.get_winners()),
                losers: Some(results.get_losers()),
            });
        }
        None
    }
}

impl Ord for RockPaperScissorsGame {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.room_address.cmp(&other.room_address)
    }
}

impl PartialOrd for RockPaperScissorsGame {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
