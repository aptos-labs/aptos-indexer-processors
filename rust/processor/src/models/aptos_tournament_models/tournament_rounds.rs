// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{models::default_models::move_resources::MoveResource, schema::tournament_rounds};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TournamentRoundMapping = HashMap<String, TournamentRound>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournament_rounds)]
pub struct TournamentRound {
    address: String,
    number: i64,
    play_started: bool,
    play_ended: bool,
    paused: bool,
    last_transaction_version: i64,
}

impl TournamentRound {
    pub fn pk(&self) -> String {
        self.address.clone()
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

        if let AptosTournamentResource::Round(inner) = AptosTournamentResource::from_resource(
            contract_addr,
            &type_str,
            resource.data.as_ref().unwrap(),
            transaction_version,
        )
        .unwrap()
        {
            return Some(TournamentRound {
                address: resource.address.clone(),
                number: inner.number,
                play_started: inner.play_started,
                play_ended: inner.play_ended,
                paused: inner.paused,
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for TournamentRound {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.address.cmp(&other.address)
    }
}

impl PartialOrd for TournamentRound {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
