// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
        aptos_tournament_models::{
            aptos_tournament_utils::{
                CreateRoomEvent, CurrentRound, CurrentRoundMapping, TokenV1RewardClaimed,
                TournamentState, TournamentStateMapping,
            },
            main_page_tournament::MainPageTournamentModel,
            rock_paper_scissors_game::{RockPaperScissorsGame, RockPaperScissorsGameMapping},
            rock_paper_scissors_player::{RockPaperScissorsPlayer, RockPaperScissorsPlayerMapping},
            roulette::{Roulette, RouletteMapping},
            tournament_coin_rewards::{TournamentCoinReward, TournamentCoinRewardMapping},
            tournament_players::{TournamentPlayer, TournamentPlayerMapping},
            tournament_rooms::{TournamentRoom, TournamentRoomMapping},
            tournament_rounds::{TournamentRound, TournamentRoundMapping},
            tournament_token_rewards::{TournamentTokenReward, TournamentTokenRewardMapping},
            tournaments::{Tournament, TournamentMapping},
            trivia_answer::{TriviaAnswer, TriviaAnswerMapping},
            trivia_question::{TriviaQuestion, TriviaQuestionMapping},
        },
        object_models::v2_object_utils::ObjectWithMetadata,
    },
    schema,
    utils::{
        database::{execute_in_chunks, PgDbPool},
        util::standardize_address,
    },
};
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use diesel::{pg::Pg, query_builder::QueryFragment, upsert::excluded, ExpressionMethods};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};

pub const CHUNK_SIZE: usize = 1000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AptosTournamentProcessorConfig {
    contract_address: String,
}

pub struct AptosTournamentProcessor {
    connection_pool: PgDbPool,
    config: AptosTournamentProcessorConfig,
}

impl AptosTournamentProcessor {
    pub fn new(connection_pool: PgDbPool, config: AptosTournamentProcessorConfig) -> Self {
        tracing::info!("init AptosTournamentProcessor");
        let mut config = config;
        config.contract_address = standardize_address(&config.contract_address);
        Self {
            connection_pool,
            config,
        }
    }
}

impl Debug for AptosTournamentProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "AptosTournamentProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: PgDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    main_page_tournaments_to_insert: Vec<MainPageTournamentModel>,
    tournaments_to_insert: Vec<Tournament>,
    tournament_coin_rewards_to_insert: Vec<TournamentCoinReward>,
    tournament_token_rewards_to_insert: Vec<TournamentTokenReward>,
    tournament_rounds_to_insert: Vec<TournamentRound>,
    tournament_rooms_to_insert: Vec<TournamentRoom>,
    tournament_rooms_to_delete: Vec<TournamentRoom>,
    tournament_players_to_insert: Vec<TournamentPlayer>,
    tournament_players_to_assign_room: Vec<TournamentPlayer>,
    tournament_players_to_delete_room: Vec<TournamentPlayer>,
    tournament_players_to_claim_coin: Vec<TournamentPlayer>,
    tournament_players_to_delete: Vec<TournamentPlayer>,
    rock_paper_scissors_games_to_insert: Vec<RockPaperScissorsGame>,
    rock_paper_scissors_results_to_insert: Vec<RockPaperScissorsGame>,
    rock_paper_scissors_players_to_insert: Vec<RockPaperScissorsPlayer>,
    trivia_questiosn_to_insert: Vec<TriviaQuestion>,
    trivia_answers_to_insert: Vec<TriviaAnswer>,
    roulette_to_insert: Vec<Roulette>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    execute_in_chunks(
        conn.clone(),
        insert_main_page_tournaments,
        main_page_tournaments_to_insert,
        MainPageTournamentModel::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournaments,
        tournaments_to_insert,
        Tournament::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_coin_rewards,
        tournament_coin_rewards_to_insert,
        TournamentCoinReward::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_token_rewards,
        tournament_token_rewards_to_insert,
        TournamentTokenReward::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_rounds,
        tournament_rounds_to_insert,
        TournamentRound::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_rooms,
        tournament_rooms_to_insert,
        TournamentRoom::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_rooms_to_delete,
        tournament_rooms_to_delete,
        TournamentRoom::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_players,
        tournament_players_to_insert,
        TournamentPlayer::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_players_to_assign_room,
        tournament_players_to_assign_room,
        TournamentPlayer::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_players_to_delete_room,
        tournament_players_to_delete_room,
        TournamentPlayer::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_players_to_claim_coin,
        tournament_players_to_claim_coin,
        TournamentPlayer::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_tournament_players_to_delete,
        tournament_players_to_delete,
        TournamentPlayer::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_rock_paper_scissors_games,
        rock_paper_scissors_games_to_insert,
        RockPaperScissorsGame::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_rock_paper_scissors_results,
        rock_paper_scissors_results_to_insert,
        RockPaperScissorsGame::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_rock_paper_scissors_players,
        rock_paper_scissors_players_to_insert,
        RockPaperScissorsPlayer::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_trivia_questions,
        trivia_questiosn_to_insert,
        TriviaQuestion::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_trivia_answers,
        trivia_answers_to_insert,
        TriviaAnswer::field_count(),
    )
    .await?;

    execute_in_chunks(
        conn.clone(),
        insert_roulette,
        roulette_to_insert,
        Roulette::field_count(),
    )
    .await?;
    Ok(())
}

fn insert_main_page_tournaments(
    main_page_tournaments_to_insert: Vec<MainPageTournamentModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    (
        diesel::insert_into(schema::main_page_tournament::table)
            .values(main_page_tournaments_to_insert)
            .on_conflict_do_nothing(),
        None,
    )
}

fn insert_tournaments(
    tournaments_to_insert: Vec<Tournament>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournaments::dsl::*;
    (
        diesel::insert_into(schema::tournaments::table)
            .values(tournaments_to_insert)
            .on_conflict(address)
            .do_update()
            .set((
                tournament_name.eq(excluded(tournament_name)),
                max_players.eq(excluded(max_players)),
                max_num_winners.eq(excluded(max_num_winners)),
                players_joined.eq(excluded(players_joined)),
                is_joinable.eq(excluded(is_joinable)),
                current_round_address.eq(excluded(current_round_address)),
                current_round_number.eq(excluded(current_round_number)),
                current_game_module.eq(excluded(current_game_module)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                tournament_ended_at.eq(excluded(tournament_ended_at)),
                inserted_at.eq(excluded(inserted_at)),
                tournament_start_timestamp.eq(excluded(tournament_start_timestamp)),
            )),
        Some(" WHERE tournaments.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_tournament_coin_rewards(
    tournament_coin_rewards_to_insert: Vec<TournamentCoinReward>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_coin_rewards::dsl::*;
    (
        diesel::insert_into(schema::tournament_coin_rewards::table)
            .values(tournament_coin_rewards_to_insert)
            .on_conflict((tournament_address, coin_type))
            .do_update()
            .set((
                coin_type.eq(excluded(coin_type)),
                coins.eq(excluded(coins)),
                coin_reward_amount.eq(excluded(coin_reward_amount)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(
            " WHERE tournament_coin_rewards.last_transaction_version <= excluded.last_transaction_version ",
        ),
    )
}

fn insert_tournament_token_rewards(
    tournament_token_rewards_to_insert: Vec<TournamentTokenReward>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_token_rewards::dsl::*;

    ( diesel::insert_into(schema::tournament_token_rewards::table)
                .values(tournament_token_rewards_to_insert)
                .on_conflict(tournament_address)
                .do_update()
                .set((
                    tokens.eq(excluded(tokens)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(
                " WHERE tournament_token_rewards.last_transaction_version <= excluded.last_transaction_version ",
            ),
        )
}

fn insert_tournament_rounds(
    tournament_rounds_to_insert: Vec<TournamentRound>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_rounds::dsl::*;

    (diesel::insert_into(schema::tournament_rounds::table)
                .values(tournament_rounds_to_insert)
                .on_conflict(address)
                .do_update()
                .set((
                    number.eq(excluded(number)),
                    play_started.eq(excluded(play_started)),
                    play_ended.eq(excluded(play_ended)),
                    paused.eq(excluded(paused)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                    round_end_timestamp.eq(excluded(round_end_timestamp))
                )),
            Some(
                " WHERE tournament_rounds.last_transaction_version <= excluded.last_transaction_version ",
            ),)
}

fn insert_tournament_rooms(
    tournament_rooms_to_insert: Vec<TournamentRoom>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_rooms::dsl::*;
    (
            diesel::insert_into(schema::tournament_rooms::table)
                .values(tournament_rooms_to_insert)
                .on_conflict(address)
                .do_update()
                .set((
                    tournament_address.eq(excluded(tournament_address)),
                    round_address.eq(excluded(round_address)),
                    in_progress.eq(excluded(in_progress)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(
                " WHERE tournament_rooms.last_transaction_version <= excluded.last_transaction_version ",
            ),)
}

fn insert_tournament_rooms_to_delete(
    tournament_rooms_to_insert: Vec<TournamentRoom>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_rooms::dsl::*;
    (
            diesel::insert_into(schema::tournament_rooms::table)
                .values(tournament_rooms_to_insert)
                .on_conflict(address)
                .do_update()
                .set((
                    in_progress.eq(excluded(in_progress)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
            Some(
                " WHERE tournament_rooms.last_transaction_version <= excluded.last_transaction_version ",
            ))
}

fn insert_tournament_players(
    tournament_players_to_insert: Vec<TournamentPlayer>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_players::dsl::*;

    (   diesel::insert_into(schema::tournament_players::table)
                .values(tournament_players_to_insert)
                .on_conflict(token_address)
                .do_update()
                .set((
                    user_address.eq(excluded(user_address)),
                    tournament_address.eq(excluded(tournament_address)),
                    // Do not update room address here; separately update in other function to reduce number of lookups
                    // room_address.eq(excluded(room_address)),
                    player_name.eq(excluded(player_name)),
                    alive.eq(excluded(alive)),
                    token_uri.eq(excluded(token_uri)),
                    coin_reward_claimed_type.eq(excluded(coin_reward_claimed_type)),
                    coin_reward_claimed_amount.eq(excluded(coin_reward_claimed_amount)),
                    token_reward_claimed.eq(excluded(token_reward_claimed)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ),)
}

fn insert_tournament_players_to_assign_room(
    tournament_players_to_insert: Vec<TournamentPlayer>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_players::dsl::*;
    (
            diesel::insert_into(schema::tournament_players::table)
                .values(tournament_players_to_insert)
                .on_conflict(token_address)
                .do_update()
                .set((
                    room_address.eq(excluded(room_address)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ),)
}

fn insert_tournament_players_to_delete_room(
    tournament_players_to_insert: Vec<TournamentPlayer>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_players::dsl::*;
    (
            diesel::insert_into(schema::tournament_players::table)
                .values(tournament_players_to_insert)
                .on_conflict(token_address)
                .do_update()
                .set((
                    room_address.eq(excluded(room_address)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ))
}

fn insert_tournament_players_to_claim_coin(
    tournament_players_to_insert: Vec<TournamentPlayer>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_players::dsl::*;

    ( diesel::insert_into(schema::tournament_players::table)
                .values(tournament_players_to_insert)
                .on_conflict(token_address)
                .do_update()
                .set((
                    coin_reward_claimed_type.eq(excluded(coin_reward_claimed_type)),
                    coin_reward_claimed_amount.eq(excluded(coin_reward_claimed_amount)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ))
}

fn insert_tournament_players_to_delete(
    tournament_players_to_insert: Vec<TournamentPlayer>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::tournament_players::dsl::*;

    (  diesel::insert_into(schema::tournament_players::table)
                .values(tournament_players_to_insert)
                .on_conflict(token_address)
                .do_update()
                .set((
                    alive.eq(excluded(alive)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ))
}

fn insert_rock_paper_scissors_games(
    rock_paper_scissors_games_to_insert: Vec<RockPaperScissorsGame>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::rock_paper_scissors_games::dsl::*;

    ( diesel::insert_into(schema::rock_paper_scissors_games::table)
                .values(rock_paper_scissors_games_to_insert)
                .on_conflict(room_address)
                .do_update()
                .set((
                    room_address.eq(excluded(room_address)),
                    player1_token_address.eq(excluded(player1_token_address)),
                    player2_token_address.eq(excluded(player2_token_address)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
                Some(
                    " WHERE rock_paper_scissors_games.last_transaction_version <= excluded.last_transaction_version ",
                ),)
}
fn insert_rock_paper_scissors_results(
    rock_paper_scissors_games_to_insert: Vec<RockPaperScissorsGame>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::rock_paper_scissors_games::dsl::*;

    (  diesel::insert_into(schema::rock_paper_scissors_games::table)
                .values(rock_paper_scissors_games_to_insert)
                .on_conflict(room_address)
                .do_update()
                .set((
                    winners.eq(excluded(winners)),
                    losers.eq(excluded(losers)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE rock_paper_scissors_games.last_transaction_version <= excluded.last_transaction_version ",
                ),)
}

fn insert_rock_paper_scissors_players(
    rock_paper_scissors_players_to_insert: Vec<RockPaperScissorsPlayer>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::rock_paper_scissors_players::dsl::*;

    (    diesel::insert_into(schema::rock_paper_scissors_players::table)
                .values(rock_paper_scissors_players_to_insert)
                .on_conflict((token_address, room_address))
                .do_update()
                .set((
                    user_address.eq(excluded(user_address)),
                    committed_action.eq(excluded(committed_action)),
                    verified_action.eq(excluded(verified_action)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
                Some(
                    " WHERE rock_paper_scissors_players.last_transaction_version <= excluded.last_transaction_version ",
                ),
            )
}

fn insert_trivia_questions(
    trivia_questiosn_to_insert: Vec<TriviaQuestion>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::trivia_questions::dsl::*;
    (
            diesel::insert_into(schema::trivia_questions::table)
                .values(trivia_questiosn_to_insert)
                .on_conflict(round_address)
                .do_update()
                .set((
                    question.eq(excluded(question)),
                    possible_answers.eq(excluded(possible_answers)),
                    revealed_answer_index.eq(excluded(revealed_answer_index)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
                Some(
                    " WHERE trivia_questions.last_transaction_version <= excluded.last_transaction_version ",
                ),
           )
}

fn insert_trivia_answers(
    trivia_answers_to_insert: Vec<TriviaAnswer>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::trivia_answers::dsl::*;
    (
        diesel::insert_into(schema::trivia_answers::table)
            .values(trivia_answers_to_insert)
            .on_conflict((token_address, round_address))
            .do_update()
            .set((
                answer_index.eq(excluded(answer_index)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(
            " WHERE trivia_answers.last_transaction_version <= excluded.last_transaction_version ",
        ),
    )
}

fn insert_roulette(
    items_to_insert: Vec<Roulette>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::roulette::dsl::*;

    (
        diesel::insert_into(schema::roulette::table)
            .values(items_to_insert)
            .on_conflict(room_address)
            .do_update()
            .set((
                result_index.eq(excluded(result_index)),
                revealed_index.eq(excluded(revealed_index)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE roulette.last_transaction_version <= excluded.last_transaction_version "),
    )
}

#[async_trait]
impl ProcessorTrait for AptosTournamentProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::AptosTournamentProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();

        let mut tournament_token_reward_claims = HashMap::new();

        let mut main_page_tournaments = HashMap::new();
        let mut tournaments: TournamentMapping = HashMap::new();
        let mut tournament_coin_rewards: TournamentCoinRewardMapping = HashMap::new();
        let mut tournament_token_rewards: TournamentTokenRewardMapping = HashMap::new();
        let mut tournament_rounds: TournamentRoundMapping = HashMap::new();
        let mut tournament_rooms: TournamentRoomMapping = HashMap::new();
        let mut tournament_rooms_to_delete: TournamentRoomMapping = HashMap::new();
        let mut tournament_players: TournamentPlayerMapping = HashMap::new();
        let mut tournament_players_to_assign_room: TournamentPlayerMapping = HashMap::new();
        let mut tournament_players_to_delete_room: TournamentPlayerMapping = HashMap::new();
        let mut tournament_players_to_claim_coin: TournamentPlayerMapping = HashMap::new();
        let mut tournament_players_to_delete: TournamentPlayerMapping = HashMap::new();
        let mut rock_paper_scissors_games: RockPaperScissorsGameMapping = HashMap::new();
        let mut rock_paper_scissors_results: RockPaperScissorsGameMapping = HashMap::new();
        let mut rock_paper_scissors_players: RockPaperScissorsPlayerMapping = HashMap::new();
        let mut trivia_questions: TriviaQuestionMapping = HashMap::new();
        let mut trivia_answers: TriviaAnswerMapping = HashMap::new();
        let mut roulette: RouletteMapping = HashMap::new();

        for txn in transactions {
            let mut tournament_state_mapping: TournamentStateMapping = HashMap::new();
            let mut current_round_mapping: CurrentRoundMapping = HashMap::new();
            let mut object_to_owner = HashMap::new();
            let mut create_room_events = HashMap::new();

            let txn_version = txn.version as i64;
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");

            if let TxnData::User(user_txn) = txn_data {
                // First pass: TournamentState, CurrentRound, object mapping, token reward claim mapping
                for wsc in transaction_info.changes.iter() {
                    if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                        let address = standardize_address(&wr.address.to_string());
                        if let Some(state) = TournamentState::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        )
                        .unwrap()
                        {
                            tournament_state_mapping.insert(address.clone(), state);
                        }
                        if let Some(state) = CurrentRound::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        )
                        .unwrap()
                        {
                            current_round_mapping.insert(address.clone(), state);
                        }
                        if let Some(object) =
                            ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                        {
                            object_to_owner
                                .insert(address.clone(), object.object_core.get_owner_address());
                        }
                        if let Some(token_reward_claimed) =
                            TokenV1RewardClaimed::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                            )
                            .unwrap()
                        {
                            tournament_token_reward_claims
                                .insert(token_reward_claimed.get_receiver_address(), address);
                        }
                    }
                }

                // Second pass: get tournament and players metadata
                for wsc in transaction_info.changes.iter() {
                    if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                        if let Some(tournament_player) = TournamentPlayer::from_tournament_token(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                            &object_to_owner,
                        ) {
                            tournament_players.insert(tournament_player.pk(), tournament_player);
                        }
                        if let Some(tournament) = Tournament::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                            tournament_state_mapping.clone(),
                            current_round_mapping.clone(),
                            txn.timestamp.clone().unwrap(),
                        ) {
                            tournaments.insert(tournament.pk(), tournament);
                        }
                    }
                }

                // Pass through events for create room events
                for event in user_txn.events.iter() {
                    if let Some(create_room) = CreateRoomEvent::from_event(
                        &self.config.contract_address,
                        event,
                        txn_version,
                    )
                    .unwrap()
                    {
                        create_room_events.insert(create_room.get_object_address(), create_room);
                    }
                    if let Some(player) = TournamentPlayer::delete_player(
                        &self.config.contract_address,
                        event,
                        txn_version,
                    ) {
                        tournament_players_to_delete.insert(player.pk(), player);
                    }
                    if let Some(room) = TournamentRoom::delete_room(
                        &self.config.contract_address,
                        event,
                        txn_version,
                    ) {
                        tournament_rooms_to_delete.insert(room.pk(), room);
                    }
                    if let Some(player) = TournamentPlayer::claim_token_reward(
                        &mut self.connection_pool.get().await?,
                        event,
                        txn_version,
                        &tournament_token_reward_claims,
                        &tournament_players,
                    )
                    .await
                    {
                        tournament_players.insert(player.pk(), player);
                    }
                    if let Some(rps_result) = RockPaperScissorsGame::from_results(
                        &self.config.contract_address,
                        event,
                        txn_version,
                    ) {
                        rock_paper_scissors_results.insert(rps_result.pk(), rps_result);
                    }

                    let players = TournamentPlayer::delete_room(
                        &self.config.contract_address,
                        event,
                        txn_version,
                    );
                    tournament_players_to_delete_room.extend(players);
                }

                // Third pass: everything else
                for wsc in transaction_info.changes.iter() {
                    if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                        if let Some(round) = TournamentRound::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            tournament_rounds.insert(round.pk(), round);
                        }
                        if let Some(room) = TournamentRoom::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                            &create_room_events,
                        ) {
                            tournament_rooms.insert(room.pk(), room);
                        }
                        if let Some(coin_reward) = TournamentCoinReward::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            tournament_coin_rewards.insert(coin_reward.pk(), coin_reward);
                        }
                        if let Some(token_reward) = TournamentTokenReward::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            tournament_token_rewards.insert(token_reward.pk(), token_reward);
                        }
                        if let Some(player) = TournamentPlayer::claim_coin_reward(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            tournament_players_to_claim_coin.insert(player.pk(), player);
                        }
                        if let Some(game) = RockPaperScissorsGame::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            rock_paper_scissors_games.insert(game.pk(), game);
                        }
                        if let Some(players) = RockPaperScissorsPlayer::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            let player1 = players.0;
                            let player2 = players.1;
                            rock_paper_scissors_players.insert(player1.pk(), player1);
                            rock_paper_scissors_players.insert(player2.pk(), player2);
                        }
                        if let Some(question) = TriviaQuestion::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            trivia_questions.insert(question.pk(), question);
                        }
                        if let Some(answer) = TriviaAnswer::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            trivia_answers.insert(answer.pk(), answer);
                        }
                        if let Some(r) = Roulette::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            roulette.insert(r.pk(), r);
                        }
                        if let Some(mpt) = MainPageTournamentModel::from_write_resource(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        ) {
                            main_page_tournaments.insert(mpt.pk(), mpt);
                        }

                        let players = TournamentPlayer::from_room(
                            &self.config.contract_address,
                            wr,
                            txn_version,
                        );
                        tournament_players_to_assign_room.extend(players);
                    }
                }
            }
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let mut main_page_tournaments = main_page_tournaments.into_values().collect::<Vec<_>>();
        let mut tournaments = tournaments.into_values().collect::<Vec<_>>();
        let mut tournament_coin_rewards = tournament_coin_rewards.into_values().collect::<Vec<_>>();
        let mut tournament_token_rewards =
            tournament_token_rewards.into_values().collect::<Vec<_>>();
        let mut tournament_rounds = tournament_rounds.into_values().collect::<Vec<_>>();
        let mut tournament_rooms = tournament_rooms.into_values().collect::<Vec<_>>();
        let mut tournament_rooms_to_delete =
            tournament_rooms_to_delete.into_values().collect::<Vec<_>>();
        let mut tournament_players = tournament_players.into_values().collect::<Vec<_>>();
        let mut tournament_players_to_assign_room = tournament_players_to_assign_room
            .into_values()
            .collect::<Vec<_>>();
        let mut tournament_players_to_delete_room = tournament_players_to_delete_room
            .into_values()
            .collect::<Vec<_>>();
        let mut tournament_players_to_claim_coin = tournament_players_to_claim_coin
            .into_values()
            .collect::<Vec<_>>();
        let mut tournament_players_to_delete = tournament_players_to_delete
            .into_values()
            .collect::<Vec<_>>();
        let mut rock_paper_scissors_games =
            rock_paper_scissors_games.into_values().collect::<Vec<_>>();
        let mut rock_paper_scissors_results = rock_paper_scissors_results
            .into_values()
            .collect::<Vec<_>>();
        let mut rock_paper_scissors_players = rock_paper_scissors_players
            .into_values()
            .collect::<Vec<_>>();
        let mut trivia_questions = trivia_questions.into_values().collect::<Vec<_>>();
        let mut trivia_answers = trivia_answers.into_values().collect::<Vec<_>>();
        let mut roulette = roulette.into_values().collect::<Vec<_>>();

        main_page_tournaments.sort();
        tournaments.sort();
        tournament_coin_rewards.sort();
        tournament_token_rewards.sort();
        tournament_rounds.sort();
        tournament_rooms.sort();
        tournament_rooms_to_delete.sort();
        tournament_players.sort();
        tournament_players_to_assign_room.sort();
        tournament_players_to_delete_room.sort();
        tournament_players_to_claim_coin.sort();
        tournament_players_to_delete.sort();
        rock_paper_scissors_games.sort();
        rock_paper_scissors_results.sort();
        rock_paper_scissors_players.sort();
        trivia_questions.sort();
        trivia_answers.sort();
        roulette.sort();

        insert_to_db(
            self.connection_pool.clone(),
            self.name(),
            start_version,
            end_version,
            main_page_tournaments,
            tournaments,
            tournament_coin_rewards,
            tournament_token_rewards,
            tournament_rounds,
            tournament_rooms,
            tournament_rooms_to_delete,
            tournament_players,
            tournament_players_to_assign_room,
            tournament_players_to_delete_room,
            tournament_players_to_claim_coin,
            tournament_players_to_delete,
            rock_paper_scissors_games,
            rock_paper_scissors_results,
            rock_paper_scissors_players,
            trivia_questions,
            trivia_answers,
            roulette,
        )
        .await?;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_insertion_duration_in_secs,
        })
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
