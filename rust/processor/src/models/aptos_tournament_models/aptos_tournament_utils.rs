use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RefVec {
    vec: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RefSelf {
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
struct GameOverEvent {
    game_address: String,
    winners: Vec<String>,
    losers: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VerifyAction {
    game_address: String,
    player_address: String,
    action: String,
    signature: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AptosTournamentEvent {
    GameOverEvent(GameOverEvent),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Refs {
    burn_ref: RefVec,
    delete_ref: RefSelf,
    extend_ref: RefSelf,
    property_mutator_ref: RefVec,
    transfer_ref: RefSelf,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPSGame {
    player1: RPSPlayer,
    player2: RPSPlayer,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CurrentRound {
    game_module: String,
    number: String,
    round_address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TournamentDirector {
    max_num_winners: u64,
    max_players: u64,
    players_joined: u64,
    secondary_admin_address: RefVec,
    tournament_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TournamentState {
    has_ended: bool,
    is_joinable: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TournamentToken {
    last_recorded_round: u64,
    room_address: String,
    tournament_address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AptosTournament {
    admin_address: RefVec,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AptosTournamentResource {
    AptosTournament(AptosTournament),
    CurrentRound(CurrentRound),
    // MatchMaker(MatchMaker), // TODO: Figure out how to handle multiple game types
    Refs(Refs),
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
            format!("{}::aptos_tournament::AptosTournament", addr),
            format!("{}::events::GameOverEvent", addr),
            format!("{}::matchmaker::MatchMaker", addr), // TODO: Figure out how to handle multiple game types
            format!("{}::object_refs::Refs", addr),
            format!("{}::rock_paper_scissor::Game", addr),
            format!("{}::round::Round", addr), // TODO: Figure out how to handle multiple game types
            format!("{}::room::Room", addr),   // TODO: Figure out how to handle multiple game types
            format!("{}::token_manager::TournamentToken", addr),
            format!("{}::tournament_manager::CurrentRound", addr),
            format!("{}::tournament_manager::TournamentDirector", addr),
            format!("{}::tournament_manager::TournamentState", addr),
        ]
        .contains(&data_type.to_string())
    }
}
