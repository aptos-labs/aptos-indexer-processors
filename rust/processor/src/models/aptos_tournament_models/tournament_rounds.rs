// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{models::default_models::move_resources::MoveResource, schema::tournament_rounds};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournament_rounds)]
pub struct TournamentRound {
    address: String,
    play_started: bool,
    play_ended: bool,
    paused: bool,
}

impl TournamentRound {
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
                play_started: inner.play_started,
                play_ended: inner.play_ended,
                paused: inner.paused,
            });
        }
        None
    }
}
