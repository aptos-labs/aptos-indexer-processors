// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{models::default_models::move_resources::MoveResource, schema::trivia_questions};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TriviaQuestionMapping = HashMap<String, TriviaQuestion>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(round_address))]
#[diesel(table_name = trivia_questions)]
pub struct TriviaQuestion {
    pub round_address: String,
    pub question: String,
    pub possible_answers: Vec<String>,
    pub revealed_answer_index: i64,
    pub last_transaction_version: i64,
}

impl TriviaQuestion {
    pub fn pk(&self) -> String {
        self.round_address.clone()
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

        if let AptosTournamentResource::TriviaQuestion(inner) =
            AptosTournamentResource::from_resource(
                contract_addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                transaction_version,
            )
            .unwrap()
        {
            return Some(TriviaQuestion {
                round_address: resource.address.clone(),
                question: inner.question,
                possible_answers: inner.possible_answers,
                revealed_answer_index: inner.revealed_answer_index,
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for TriviaQuestion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.round_address.cmp(&other.round_address)
    }
}

impl PartialOrd for TriviaQuestion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
