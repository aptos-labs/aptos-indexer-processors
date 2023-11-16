// Copyright © Aptos Foundation

use super::aptos_tournament_utils::{Round, AptosTournamentResource};
use crate::{schema::tournament_rounds, models::default_models::move_resources::MoveResource};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournament_rounds)]
pub struct TournamentRound {
    address: String,
    matchmaking_ended: bool,
    play_started: bool,
    play_ended: bool,
    paused: bool,
    matchmaker_address: String,
}

impl TournamentRound {
    pub fn from_write_resource(
        contract_addr: &str,
        write_resource: &WriteResource,
        transaction_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(contract_addr, type_str.as_str()) {
            return Ok(None);
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
        )? {
            Ok(Some(TournamentRound {
                address: resource.address.clone(),
                matchmaking_ended: inner.matchmaking_ended,
                play_started: inner.play_started,
                play_ended: inner.play_ended,
                paused: inner.paused,
                matchmaker_address: inner.get_matchmaker_address(),
            }))
        } else {
            Ok(None)
        }
    }
}
