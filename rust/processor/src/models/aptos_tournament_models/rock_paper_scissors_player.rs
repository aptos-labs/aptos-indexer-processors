// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{
    models::default_models::move_resources::MoveResource, schema::rock_paper_scissors_players,
};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type RockPaperScissorsPlayerMapping = HashMap<(String, String), RockPaperScissorsPlayer>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(token_address, room_address))]
#[diesel(table_name = rock_paper_scissors_players)]
pub struct RockPaperScissorsPlayer {
    pub token_address: String,
    pub room_address: String,
    pub user_address: String,
    pub committed_action: Option<Vec<String>>,
    pub verified_action: Option<Vec<String>>,
    pub last_transaction_version: i64,
}

impl RockPaperScissorsPlayer {
    pub fn pk(&self) -> (String, String) {
        (self.token_address.clone(), self.room_address.clone())
    }

    pub fn from_write_resource(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
    ) -> Option<(Self, Self)> {
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
            let mut p1_committed_action = None;
            if let Some(committed_action) = inner.player1.clone().committed_action {
                p1_committed_action = Some(committed_action.vec);
            }
            let mut p2_committed_action = None;
            if let Some(committed_action) = inner.player2.clone().committed_action {
                p2_committed_action = Some(committed_action.vec);
            }

            let mut p1_verified_action = None;
            if let Some(verified_action) = inner.player1.clone().verified_action {
                p1_verified_action = Some(verified_action.vec);
            }
            let mut p2_verified_action = None;
            if let Some(verified_action) = inner.player2.clone().verified_action {
                p2_verified_action = Some(verified_action.vec);
            }

            return Some((
                RockPaperScissorsPlayer {
                    token_address: inner.player1.get_token_address(),
                    room_address: resource.address.clone(),
                    user_address: inner.player1.get_address(),
                    committed_action: p1_committed_action,
                    verified_action: p1_verified_action,
                    last_transaction_version: transaction_version,
                },
                RockPaperScissorsPlayer {
                    token_address: inner.player2.get_token_address(),
                    room_address: resource.address.clone(),
                    user_address: inner.player2.get_address(),
                    committed_action: p2_committed_action,
                    verified_action: p2_verified_action,
                    last_transaction_version: transaction_version,
                },
            ));
        }
        None
    }
}

impl Ord for RockPaperScissorsPlayer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_address.cmp(&other.token_address)
    }
}

impl PartialOrd for RockPaperScissorsPlayer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
