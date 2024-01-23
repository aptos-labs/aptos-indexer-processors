use crate::{
    models::{
        coin_models::coin_utils::Coin, default_models::move_resources::MoveResource,
        token_models::token_utils::TokenType,
    },
    utils::util::{deserialize_from_string, standardize_address, standardized_type_address},
};
use anyhow::Context;
use aptos_protos::transaction::v1::{Event, WriteResource};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    pub token_uri: String,
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
pub struct CoinRewardPool {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub coin_reward_amount: i64,
    pub coins: Coin,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CoinRewardClaimed {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub amount: i64,
}

impl CoinRewardClaimed {
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

        if let AptosTournamentResource::CoinRewardClaimed(inner) =
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
pub struct TokenReward {
    pub inline_vec: Vec<TokenType>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenV1RewardPool {
    pub tokens: TokenReward,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenV1RewardClaimed {
    receiver_address: String,
}

impl TokenV1RewardClaimed {
    pub fn get_receiver_address(&self) -> String {
        standardize_address(&self.receiver_address)
    }
}

impl TokenV1RewardClaimed {
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

        if let AptosTournamentResource::TokenV1RewardClaimed(inner) =
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
pub struct RPSAction {
    pub vec: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPSPlayer {
    address: String,
    token_address: String,
    pub committed_action: Option<RPSAction>,
    pub verified_action: Option<RPSAction>,
}

impl RPSPlayer {
    pub fn get_address(&self) -> String {
        standardize_address(&self.address)
    }

    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token_address)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RockPaperScissors {
    pub player1: RPSPlayer,
    pub player2: RPSPlayer,
}

impl RockPaperScissors {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(write_resource, 0, txn_version, 0);

        if let AptosTournamentResource::RockPaperScissors(inner) =
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
pub struct TriviaQuestion {
    pub possible_answers: Vec<String>,
    pub question: String,
    pub revealed_answer_index: i64,
}

impl TriviaQuestion {
    pub fn from_write_resource(
        addr: &str,
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !AptosTournamentResource::is_resource_supported(addr, type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(write_resource, 0, txn_version, 0);

        if let AptosTournamentResource::TriviaQuestion(inner) =
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
pub struct TriviaAnswer {
    round_address: String,
    pub answer_index: i64,
}

impl TriviaAnswer {
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
        let resource = MoveResource::from_write_resource(write_resource, 0, txn_version, 0);

        if let AptosTournamentResource::TriviaAnswer(inner) =
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
struct RoulettePlayer {
    address: String,
    token_address: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub index: i64,
}

impl RoulettePlayer {
    pub fn get_with_standardized_address(&self) -> Self {
        Self {
            address: standardize_address(&self.address),
            token_address: standardize_address(&self.token_address),
            index: self.index.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Roulette {
    players: Vec<RoulettePlayer>,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub revealed_index: i64,
}

impl Roulette {
    pub fn get_players_json(&self) -> Value {
        let mut players = vec![];
        for player in &self.players {
            players.push(player.get_with_standardized_address());
        }
        serde_json::to_value(&players).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AptosTournamentResource {
    CurrentRound(CurrentRound),
    Room(Room),
    Round(Round),
    TournamentDirector(TournamentDirector),
    TournamentState(TournamentState),
    TournamentPlayerToken(TournamentPlayerToken),
    CoinRewardPool(CoinRewardPool),
    CoinRewardClaimed(CoinRewardClaimed),
    TokenV1RewardPool(TokenV1RewardPool),
    TokenV1RewardClaimed(TokenV1RewardClaimed),
    // MANUAL GAME INDEXING
    RockPaperScissors(RockPaperScissors),
    TriviaQuestion(TriviaQuestion),
    TriviaAnswer(TriviaAnswer),
    Roulette(Roulette),
}

impl AptosTournamentResource {
    pub fn is_resource_supported(contract_addr: &str, data_type: &str) -> bool {
        let data_type = remove_type_from_resource(data_type);
        [
            format!("{}::round::Round", contract_addr),
            format!("{}::room::Room", contract_addr),
            format!("{}::token_manager::TournamentPlayerToken", contract_addr),
            format!("{}::tournament_manager::CurrentRound", contract_addr),
            format!("{}::tournament_manager::TournamentDirector", contract_addr),
            format!("{}::tournament_manager::TournamentState", contract_addr),
            format!("{}::rewards::CoinRewardPool", contract_addr),
            format!("{}::rewards::CoinRewardClaimed", contract_addr),
            format!("{}::rewards::TokenV1RewardPool", contract_addr),
            format!("{}::rewards::TokenV1RewardClaimed", contract_addr),
            // MANUAL GAME INDEXING
            format!("{}::rock_paper_scissors::RockPaperScissors", contract_addr),
            format!("{}::trivia::TriviaQuestion", contract_addr),
            format!("{}::trivia::TriviaAnswer", contract_addr),
            format!("{}::roulette::Roulette", contract_addr),
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
            x if x == format!("{}::round::Round", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::Round(inner)))
            },
            x if x == format!("{}::rewards::CoinRewardPool", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::CoinRewardPool(inner)))
            },
            x if x == format!("{}::rewards::CoinRewardClaimed", contract_addr) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(Self::CoinRewardClaimed(inner)))
            },
            x if x == format!("{}::rewards::TokenV1RewardPool", contract_addr) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(Self::TokenV1RewardPool(inner)))
            },
            x if x == format!("{}::rewards::TokenV1RewardClaimed", contract_addr) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(Self::TokenV1RewardClaimed(inner)))
            },
            // MANUAL GAME INDEXING
            x if x == format!("{}::rock_paper_scissors::RockPaperScissors", contract_addr) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(Self::RockPaperScissors(inner)))
            },
            x if x == format!("{}::trivia::TriviaQuestion", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::TriviaQuestion(inner)))
            },
            x if x == format!("{}::trivia::TriviaAnswer", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::TriviaAnswer(inner)))
            },
            x if x == format!("{}::roulette::Roulette", contract_addr) => {
                serde_json::from_value(data.clone()).map(|inner| Some(Self::Roulette(inner)))
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
pub struct RPSResultEvent {
    winners: Vec<String>,
    losers: Vec<String>,
    game_address: String,
}

impl RPSResultEvent {
    pub fn get_game_address(&self) -> String {
        standardize_address(&self.game_address)
    }

    pub fn get_winners(&self) -> Vec<Option<String>> {
        let mut winners = vec![];
        for winner in &self.winners {
            winners.push(Some(standardize_address(winner)));
        }
        winners
    }

    pub fn get_losers(&self) -> Vec<Option<String>> {
        let mut losers = vec![];
        for loser in &self.losers {
            losers.push(Some(standardize_address(loser)));
        }
        losers
    }

    pub fn from_event(
        contract_addr: &str,
        event: &Event,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(AptosTournamentEvent::RPSResultEvent(inner)) = AptosTournamentEvent::from_event(
            contract_addr,
            &standardized_type_address(event.type_str.as_str()),
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
pub struct CreateRoomEvent {
    tournament_address: String,
    current_round_address: String,
}

impl CreateRoomEvent {
    pub fn get_tournament_address(&self) -> String {
        standardize_address(&self.tournament_address)
    }

    pub fn get_current_round_address(&self) -> String {
        standardize_address(&self.current_round_address)
    }

    pub fn from_event(
        contract_addr: &str,
        event: &Event,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(AptosTournamentEvent::CreateRoomEvent(inner)) =
            AptosTournamentEvent::from_event(
                contract_addr,
                &standardized_type_address(event.type_str.as_str()),
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
                &standardized_type_address(event.type_str.as_str()),
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
    players: RoomPlayers,
}

impl BurnRoomEvent {
    pub fn get_object_address(&self) -> String {
        standardize_address(&self.object_address)
    }

    pub fn get_players(&self) -> Vec<String> {
        let mut players = vec![];
        for player_vec in &self.players.vec {
            for player in player_vec {
                players.push(player.get_address());
            }
        }
        players
    }

    pub fn from_event(
        contract_addr: &str,
        event: &Event,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(AptosTournamentEvent::BurnRoomEvent(inner)) = AptosTournamentEvent::from_event(
            contract_addr,
            &standardized_type_address(event.type_str.as_str()),
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
    RPSResultEvent(RPSResultEvent),
    CreateRoomEvent(CreateRoomEvent),
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
            x if x == format!("{}::rock_paper_scissors::RPSResultEvent", contract_addr) => {
                serde_json::from_str(event_data).map(|inner| Some(Self::RPSResultEvent(inner)))
            },
            x if x == format!("{}::room::CreateRoomEvent", contract_addr) => {
                serde_json::from_str(event_data).map(|inner| Some(Self::CreateRoomEvent(inner)))
            },
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
