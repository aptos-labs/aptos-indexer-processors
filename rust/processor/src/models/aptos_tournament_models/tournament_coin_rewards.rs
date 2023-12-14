// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{
    models::default_models::move_resources::MoveResource, schema::tournament_coin_rewards,
};
use aptos_protos::transaction::v1::WriteResource;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TournamentCoinRewardMapping = HashMap<String, TournamentCoinReward>;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
#[diesel(primary_key(tournament_address, coin_type))]
#[diesel(table_name = tournament_coin_rewards)]
pub struct TournamentCoinReward {
    pub tournament_address: String,
    pub coin_type: String,
    pub coins: BigDecimal,
    pub coin_reward_amount: i64,
    pub last_transaction_version: i64,
}

impl TournamentCoinReward {
    pub fn pk(&self) -> (String, String) {
        (self.tournament_address.clone(), self.coin_type.clone())
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

        if let AptosTournamentResource::CoinRewardPool(inner) =
            AptosTournamentResource::from_resource(
                contract_addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                transaction_version,
            )
            .unwrap()
        {
            return Some(TournamentCoinReward {
                tournament_address: resource.address.clone(),
                coin_type: get_coin_type(resource),
                coins: inner.coins.value,
                coin_reward_amount: inner.coin_reward_amount,
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for TournamentCoinReward {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tournament_address.cmp(&other.tournament_address)
    }
}

impl PartialOrd for TournamentCoinReward {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn get_coin_type(resource: MoveResource) -> String {
    let coin_type = &resource.generic_type_params.unwrap()[0]["struct"];
    format!(
        "{}::{}::{}",
        coin_type["address"].as_str().unwrap(),
        coin_type["module"].as_str().unwrap(),
        coin_type["name"].as_str().unwrap()
    )
}
