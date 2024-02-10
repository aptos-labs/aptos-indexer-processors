// Copyright © Aptos Foundation

use super::aptos_tournament_utils::AptosTournamentResource;
use crate::{models::default_models::move_resources::MoveResource, schema::main_page_tournament};
use aptos_protos::transaction::v1::WriteResource;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, PartialEq, Eq,
)]
#[diesel(primary_key(tournament_address))]
#[diesel(table_name = main_page_tournament)]
pub struct MainPageTournament {
    pub tournament_address: String,
    pub last_transaction_version: i64,
}

impl MainPageTournament {
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

        if let AptosTournamentResource::MainPageTournament(inner) =
            AptosTournamentResource::from_resource(
                contract_addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                transaction_version,
            )
            .unwrap()
        {
            return Some(MainPageTournament {
                tournament_address: inner.get_tournament_address(),
                last_transaction_version: transaction_version,
            });
        }
        None
    }
}

impl Ord for MainPageTournament {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tournament_address.cmp(&other.tournament_address)
    }
}

impl PartialOrd for MainPageTournament {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub type MainPageTournamentModel = MainPageTournament;
