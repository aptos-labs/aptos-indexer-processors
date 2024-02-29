// Copyright © Aptos Foundation

use crate::{
    models::{
        aptos_tournament_models::aptos_tournament_utils::AptosTournamentResource,
        default_models::move_resources::MoveResource,
    },
    schema::roulette,
};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub type RouletteMapping = HashMap<String, Roulette>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(room_address))]
#[diesel(table_name = roulette)]
pub struct Roulette {
    pub room_address: String,
    pub result_index: Value,
    pub revealed_index: i64,
    pub last_transaction_version: i64,
}

impl Roulette {
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

        if let AptosTournamentResource::Roulette(inner) = AptosTournamentResource::from_resource(
            contract_addr,
            &type_str,
            resource.data.as_ref().unwrap(),
            transaction_version,
        )
        .unwrap()
        {
            return Some(Roulette {
                room_address: resource.address.clone(),
                revealed_index: inner.revealed_index,
                result_index: inner.get_players_json(),
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for Roulette {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.pk()).cmp(&other.pk())
    }
}

impl PartialOrd for Roulette {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
