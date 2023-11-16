// Copyright © Aptos Foundation

use crate::schema::tournament_rounds;
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use super::aptos_tournament_utils::Round;

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
    ) -> Option<Self> {
        if let Some(r) =
            Round::from_write_resource(contract_addr, write_resource, transaction_version).unwrap()
        {
            return Some(TournamentRound {
                address: write_resource.address.clone(),
                matchmaking_ended: r.matchmaking_ended,
                play_started: r.play_started,
                play_ended: r.play_ended,
                paused: r.paused,
                matchmaker_address: r.get_matchmaker_address(),
            });
        }
        None
    }
}
