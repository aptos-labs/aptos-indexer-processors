// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{models::default_models::move_resources::MoveResource, schema::trivia_answers};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TriviaAnswerMapping = HashMap<(String, String), TriviaAnswer>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(token_address, round_address))]
#[diesel(table_name = trivia_answers)]
pub struct TriviaAnswer {
    pub token_address: String,
    pub round_address: String,
    pub answer_index: i64,
    pub last_transaction_version: i64,
}

impl TriviaAnswer {
    pub fn pk(&self) -> (String, String) {
        (self.token_address.clone(), self.round_address.clone())
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

        if let AptosTournamentResource::TriviaAnswer(inner) =
            AptosTournamentResource::from_resource(
                contract_addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                transaction_version,
            )
            .unwrap()
        {
            return Some(TriviaAnswer {
                token_address: resource.address.clone(),
                round_address: inner.get_round_address(),
                answer_index: inner.answer_index,
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for TriviaAnswer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_address.cmp(&other.token_address)
    }
}

impl PartialOrd for TriviaAnswer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
