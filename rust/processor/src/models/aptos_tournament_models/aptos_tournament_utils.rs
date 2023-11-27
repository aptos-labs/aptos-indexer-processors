use crate::{
    models::default_models::move_resources::MoveResource,
    utils::util::{deserialize_from_string, standardize_address},
};
use anyhow::Context;
use aptos_protos::transaction::v1::{Event, WriteResource};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type TournamentStateMapping = HashMap<String, TournamentState>;
pub type CurrentRoundMapping = HashMap<String, CurrentRound>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CurrentRound {
    pub game_module: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub number: i64,
    round_address: String,
}

impl CurrentRound {
    pub fn get_round_address(&self) -> String {
        standardize_address(&self.round_address)
    }

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
    #[serde(deserialize_with = "deserialize_from_string")]
    pub max_num_winners: i64,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub max_players: i64,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub players_joined: i64,
    pub tournament_name: String,
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
    pub has_ended: bool,
    pub is_joinable: bool,
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
pub struct TournamentPlayerToken {
    pub player_name: String,
    tournament_address: String,
}

impl TournamentPlayerToken {
    pub fn get_tournament_address(&self) -> String {
        standardize_address(&self.tournament_address)
    }

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

        if let AptosTournamentResource::TournamentPlayerToken(inner) =
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
pub struct MatchMaker {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub min_players_per_room: i64,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub max_players_per_room: i64,
    pub joining_allowed: bool,
}

impl MatchMaker {
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

        if let AptosTournamentResource::MatchMaker(inner) = AptosTournamentResource::from_resource(
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
struct Player {
    inner: String,
}

impl Player {
    fn get_address(&self) -> String {
        standardize_address(&self.inner)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RoomPlayers {
    vec: Vec<Vec<Player>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Room {
    players: RoomPlayers,
}

impl Room {
    pub fn get_players(&self) -> Vec<String> {
        let mut players = vec![];
        for player_vec in &self.players.vec {
            for player in player_vec {
                players.push(player.get_address());
            }
        }
        players
    }
}

impl Room {
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

        if let AptosTournamentResource::Room(inner) = AptosTournamentResource::from_resource(
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
pub struct Round {
    pub matchmaking_ended: bool,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub number: i64,
    pub paused: bool,
    pub play_ended: bool,
    pub play_started: bool,
    matchmaker_address: String,
}

impl Round {
    pub fn get_matchmaker_address(&self) -> String {
        standardize_address(&self.matchmaker_address)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AptosTournamentResource {
    CurrentRound(CurrentRound),
    MatchMaker(MatchMaker),
    Room(Room),
    Round(Round),
    TournamentDirector(TournamentDirector),
    TournamentState(TournamentState),
    TournamentPlayerToken(TournamentPlayerToken),
}

impl AptosTournamentResource {
    pub fn is_resource_supported(contract_addr: &str, data_type: &str) -> bool {
        let data_type = remove_type_from_resource(data_type);
        [
            format!("{}::matchmaker::MatchMaker", contract_addr),
            format!("{}::round::Round", contract_addr),
            format!("{}::room::Room", contract_addr),
            format!("{}::token_manager::TournamentPlayerToken", contract_addr),
            format!("{}::tournament_manager::CurrentRound", contract_addr),
            format!("{}::tournament_manager::TournamentDirector", contract_addr),
            format!("{}::tournament_manager::TournamentState", contract_addr),
        ]
        .contains(&data_type.to_string())
    }

    pub fn from_resource(
        contract_addr: &str,
        data_type: &str,
        data: &serde_json::Value,
        txn_version: i64,
    ) -> anyhow::Result<Self> {
        let data_type = remove_type_from_resource(data_type);
        match data_type.clone() {
            x if x == format!("{}::token_manager::TournamentPlayerToken", contract_addr) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(Self::TournamentPlayerToken(inner)))
            },
            x if x == format!("{}::tournament_manager::CurrentRound", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::CurrentRound(inner)))
            },
            x if x == format!("{}::tournament_manager::TournamentDirector", contract_addr) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(Self::TournamentDirector(inner)))
            },
            x if x == format!("{}::tournament_manager::TournamentState", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::TournamentState(inner)))
            },
            x if x == format!("{}::room::Room", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::Room(inner)))
            },
            x if x == format!("{}::matchmaker::MatchMaker", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::MatchMaker(inner)))
            },
            x if x == format!("{}::round::Round", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::Round(inner)))
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

fn remove_type_from_resource(data_type: &str) -> String {
    let split: Vec<&str> = data_type.split('<').collect();
    split[0].to_string()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BurnPlayerTokenEvent {
    object_address: String,
}

impl BurnPlayerTokenEvent {
    pub fn get_object_address(&self) -> String {
        standardize_address(&self.object_address)
    }

    pub fn from_event(
        contract_addr: &str,
        event: &Event,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(AptosTournamentEvent::BurnPlayerTokenEvent(inner)) =
            AptosTournamentEvent::from_event(
                contract_addr,
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
pub struct BurnRoomEvent {
    object_address: String,
}

impl BurnRoomEvent {
    pub fn get_object_address(&self) -> String {
        standardize_address(&self.object_address)
    }

    pub fn from_event(
        contract_addr: &str,
        event: &Event,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(AptosTournamentEvent::BurnRoomEvent(inner)) = AptosTournamentEvent::from_event(
            contract_addr,
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
    BurnPlayerTokenEvent(BurnPlayerTokenEvent),
    BurnRoomEvent(BurnRoomEvent),
}

impl AptosTournamentEvent {
    pub fn from_event(
        contract_addr: &str,
        event_type: &str,
        event_data: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        match event_type {
            x if x == format!("{}::token_manager::BurnPlayerTokenEvent", contract_addr) => {
                serde_json::from_str(event_data)
                    .map(|inner| Some(Self::BurnPlayerTokenEvent(inner)))
            },
            x if x == format!("{}::room::BurnRoomEvent", contract_addr) => {
                serde_json::from_str(event_data).map(|inner| Some(Self::BurnRoomEvent(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, event_type, event_data
        ))
    }
}
