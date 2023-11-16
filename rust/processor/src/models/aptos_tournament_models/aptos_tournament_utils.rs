use crate::models::{
    default_models::move_resources::MoveResource,
    token_v2_models::v2_token_utils::ObjectWithMetadata,
};
use anyhow::Context;
use aptos_protos::transaction::v1::{Event, WriteResource};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tracks all tournament related data in a hashmap for quick access (keyed on address of the object core)
// Do I need this lol.. i'm just copying token v2
pub type AptosTournamentAggregatedDataMapping = HashMap<String, AptosTournamentAggregatedData>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AptosTournamentAggregatedData {
    pub aptos_tournament: Option<AptosTournament>,
    pub current_round: Option<CurrentRound>,
    // pub refs: Option<Refs>,
    pub rps_game: Option<RPSGame>,
    pub tournament_director: Option<TournamentDirector>,
    pub tournament_state: Option<TournamentState>,
    pub tournament_token: Option<TournamentToken>,
    pub object: ObjectWithMetadata,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RefVec {
    vec: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RefSelf {
    #[serde(rename = "self")]
    self_: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPSPlayer {
    address: String,
    committed_action: RefVec,
    token_address: String,
    verified_action: RefVec,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GameOverEvent {
    game_address: String,
    winners: Vec<String>,
    losers: Vec<String>,
}

impl GameOverEvent {
    pub fn from_event(addr: &str, event: &Event, txn_version: i64) -> anyhow::Result<Option<Self>> {
        if let Some(AptosTournamentEvent::GameOverEvent(inner)) = AptosTournamentEvent::from_event(
            addr,
            event.type_str.as_str(),
            &event.data,
            txn_version,
        )
        .unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AptosTournamentEvent {
    GameOverEvent(GameOverEvent),
}

impl AptosTournamentEvent {
    pub fn from_event(
        addr: &str,
        data_type: &str,
        data: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        match data_type {
            x if x == format!("{}::object::GameOverEvent", addr) => {
                serde_json::from_str(data).map(|inner| Some(Self::GameOverEvent(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type, data
        ))
    }
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct BurnRef {
//     vec: Vec<BurnRef2>,
// }

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct Refs {
//     burn_ref: BurnRef,
//     delete_ref: RefSelf,
//     extend_ref: RefSelf,
//     property_mutator_ref: RefVec,
//     transfer_ref: RefSelf,
// }

// impl Refs {
//     pub fn from_write_resource(
//         addr: &str,
//         write_resource: &WriteResource,
//         txn_version: i64,
//     ) -> anyhow::Result<Option<Self>> {
//         let type_str = MoveResource::get_outer_type_from_resource(write_resource);
//         if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
//             return Ok(None);
//         }
//         let resource = MoveResource::from_write_resource(
//             write_resource,
//             0, // Placeholder, this isn't used anyway
//             txn_version,
//             0, // Placeholder, this isn't used anyway
//         );

//         if let AptosTournamentResource::Refs(inner) = AptosTournamentResource::from_resource(
//             addr,
//             &type_str,
//             resource.data.as_ref().unwrap(),
//             txn_version,
//         )? {
//             Ok(Some(inner))
//         } else {
//             Ok(None)
//         }
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPSGame {
    player1: RPSPlayer,
    player2: RPSPlayer,
}

impl RPSGame {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let AptosTournamentResource::RPSGame(inner) = AptosTournamentResource::from_resource(
            addr,
            &type_str,
            resource.data.as_ref().unwrap(),
            txn_version,
        )? {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CurrentRound {
    game_module: String,
    number: String,
    round_address: String,
}

impl CurrentRound {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let AptosTournamentResource::CurrentRound(inner) =
            AptosTournamentResource::from_resource(
                addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                txn_version,
            )?
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TournamentDirector {
    max_num_winners: String,
    max_players: String,
    players_joined: String,
    secondary_admin_address: RefVec,
    tournament_name: String,
}

impl TournamentDirector {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let AptosTournamentResource::TournamentDirector(inner) =
            AptosTournamentResource::from_resource(
                addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                txn_version,
            )?
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TournamentState {
    has_ended: bool,
    is_joinable: bool,
}

impl TournamentState {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let AptosTournamentResource::TournamentState(inner) =
            AptosTournamentResource::from_resource(
                addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                txn_version,
            )?
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TournamentToken {
    last_recorded_round: String,
    room_address: String,
    tournament_address: String,
}

impl TournamentToken {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let AptosTournamentResource::TournamentToken(inner) =
            AptosTournamentResource::from_resource(
                addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                txn_version,
            )?
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AptosTournament {
    admin_address: RefVec,
}

impl AptosTournament {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let AptosTournamentResource::AptosTournament(inner) =
            AptosTournamentResource::from_resource(
                addr,
                &type_str,
                resource.data.as_ref().unwrap(),
                txn_version,
            )?
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AptosTournamentResource {
    AptosTournament(AptosTournament),
    CurrentRound(CurrentRound),
    // MatchMaker(MatchMaker), // TODO: Figure out how to handle multiple game types
    // Refs(Refs), // TODO: Figure out how to flatten them or better index nested structs
    // Room(Room),       // TODO: Figure out how to handle multiple game types
    // Round(Round), // TODO: Figure out how to handle multiple game types
    RPSGame(RPSGame), // TODO: Figure out how to handle multiple game types
    TournamentDirector(TournamentDirector),
    TournamentState(TournamentState),
    TournamentToken(TournamentToken),
}

impl AptosTournamentResource {
    pub fn is_resource_supported(addr: &str, data_type: &str) -> bool {
        [
            // format!("{}::object_refs::Refs", addr),
            // format!("{}::matchmaker::MatchMaker", addr), // TODO: Figure out how to handle multiple game types
            // format!("{}::round::Round", addr), // TODO: Figure out how to handle multiple game types
            // format!("{}::room::Room", addr),   // TODO: Figure out how to handle multiple game types
            format!("{}::aptos_tournament::AptosTournament", addr),
            format!("{}::events::GameOverEvent", addr),
            format!("{}::rock_paper_scissor::Game", addr),
            format!("{}::token_manager::TournamentToken", addr),
            format!("{}::tournament_manager::CurrentRound", addr),
            format!("{}::tournament_manager::TournamentDirector", addr),
            format!("{}::tournament_manager::TournamentState", addr),
        ]
        .contains(&data_type.to_string())
    }

    pub fn from_resource(
        addr: &str,
        data_type: &str,
        data: &serde_json::Value,
        txn_version: i64,
    ) -> anyhow::Result<Self> {
        match data_type {
            x if x == format!("{}::aptos_tournament::AptosTournament", addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::AptosTournament(inner)))
            },
            // x if x == format!("{}::object_refs::Refs", addr) => {
            //     serde_json::from_value(data.clone()).map(|inner| Some(Self::Refs(inner)))
            // },
            x if x == format!("{}::rock_paper_scissor::Game", addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::RPSGame(inner)))
            },
            x if x == format!("{}::token_manager::TournamentToken", addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::TournamentToken(inner)))
            },
            x if x == format!("{}::tournament_manager::CurrentRound", addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::CurrentRound(inner)))
            },
            x if x == format!("{}::tournament_manager::TournamentDirector", addr) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(Self::TournamentDirector(inner)))
            },
            x if x == format!("{}::tournament_manager::TournamentState", addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::TournamentState(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type, data
        ))?
        .context(format!(
            "Resource unsupported! Call is_resource_supported first. version {} type {}",
            txn_version, data_type
        ))
    }
}
