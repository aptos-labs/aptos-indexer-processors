// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{
    models::default_models::move_resources::MoveResource, schema::tournament_token_rewards,
};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TournamentTokenRewardMapping = HashMap<String, TournamentTokenReward>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(tournament_address))]
#[diesel(table_name = tournament_token_rewards)]
pub struct TournamentTokenReward {
    pub tournament_address: String,
    pub tokens: Vec<String>,
    pub last_transaction_version: i64,
}

impl TournamentTokenReward {
    pub fn pk(&self) -> String {
        self.tournament_address.clone()
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

        if let AptosTournamentResource::TokenV1RewardPool(inner) =
            AptosTournamentResource::from_resource(
                contract_addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                transaction_version,
            )
            .unwrap()
        {
            return Some(TournamentTokenReward {
                tournament_address: resource.address.clone(),
                tokens: inner
                    .tokens
                    .inline_vec
                    .iter()
                    .map(|token| token.id.token_data_id.to_hash())
                    .collect(),
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for TournamentTokenReward {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tournament_address.cmp(&other.tournament_address)
    }
}

impl PartialOrd for TournamentTokenReward {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
