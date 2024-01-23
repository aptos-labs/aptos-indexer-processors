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
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
            PgPoolConnection,
        },
        util::standardize_address,
    },
};
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use diesel::{result::Error, upsert::excluded, ExpressionMethods};
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
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
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
    roulette: Vec<Roulette>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            Box::pin(insert_to_db_impl(
                pg_conn,
                &tournaments_to_insert,
                &tournament_coin_rewards_to_insert,
                &tournament_token_rewards_to_insert,
                &tournament_rounds_to_insert,
                &tournament_rooms_to_insert,
                &tournament_rooms_to_delete,
                &tournament_players_to_insert,
                &tournament_players_to_assign_room,
                &tournament_players_to_delete_room,
                &tournament_players_to_claim_coin,
                &tournament_players_to_delete,
                &rock_paper_scissors_games_to_insert,
                &rock_paper_scissors_results_to_insert,
                &rock_paper_scissors_players_to_insert,
                &trivia_questiosn_to_insert,
                &trivia_answers_to_insert,
                &roulette,
            ))
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => {
            conn.build_transaction()
                .read_write()
                .run::<_, Error, _>(|pg_conn| {
                    Box::pin(async move {
                        let tournaments_to_insert = clean_data_for_db(tournaments_to_insert, true);
                        let tournament_coin_rewards_to_insert =
                            clean_data_for_db(tournament_coin_rewards_to_insert, true);
                        let tournament_token_rewards_to_insert =
                            clean_data_for_db(tournament_token_rewards_to_insert, true);
                        let tournament_rounds_to_insert =
                            clean_data_for_db(tournament_rounds_to_insert, true);
                        let tournament_rooms_to_insert =
                            clean_data_for_db(tournament_rooms_to_insert, true);
                        let tournament_rooms_to_delete =
                            clean_data_for_db(tournament_rooms_to_delete, true);
                        let tournament_players_to_insert =
                            clean_data_for_db(tournament_players_to_insert, true);
                        let tournament_players_to_assign_room =
                            clean_data_for_db(tournament_players_to_assign_room, true);
                        let tournament_players_to_delete_room =
                            clean_data_for_db(tournament_players_to_delete_room, true);
                        let tournament_players_to_claim_coin =
                            clean_data_for_db(tournament_players_to_claim_coin, true);
                        let tournament_players_to_delete =
                            clean_data_for_db(tournament_players_to_delete, true);
                        let rock_paper_scissors_games_to_insert =
                            clean_data_for_db(rock_paper_scissors_games_to_insert, true);
                        let rock_paper_scissors_results_to_insert =
                            clean_data_for_db(rock_paper_scissors_results_to_insert, true);
                        let rock_paper_scissors_players_to_insert =
                            clean_data_for_db(rock_paper_scissors_players_to_insert, true);
                        let trivia_questiosn_to_insert =
                            clean_data_for_db(trivia_questiosn_to_insert, true);
                        let trivia_answers_to_insert =
                            clean_data_for_db(trivia_answers_to_insert, true);
                        let roulette = clean_data_for_db(roulette, true);
                        insert_to_db_impl(
                            pg_conn,
                            &tournaments_to_insert,
                            &tournament_coin_rewards_to_insert,
                            &tournament_token_rewards_to_insert,
                            &tournament_rounds_to_insert,
                            &tournament_rooms_to_insert,
                            &tournament_rooms_to_delete,
                            &tournament_players_to_insert,
                            &tournament_players_to_assign_room,
                            &tournament_players_to_delete_room,
                            &tournament_players_to_claim_coin,
                            &tournament_players_to_delete,
                            &rock_paper_scissors_games_to_insert,
                            &rock_paper_scissors_results_to_insert,
                            &rock_paper_scissors_players_to_insert,
                            &trivia_questiosn_to_insert,
                            &trivia_answers_to_insert,
                            &roulette,
                        )
                        .await
                    })
                })
                .await
        },
    }
}

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    tournaments_to_insert: &[Tournament],
    tournament_coin_rewards_to_insert: &[TournamentCoinReward],
    tournament_token_rewards_to_insert: &[TournamentTokenReward],
    tournament_rounds_to_insert: &[TournamentRound],
    tournament_rooms_to_insert: &[TournamentRoom],
    tournament_rooms_to_delete: &[TournamentRoom],
    tournament_players_to_insert: &[TournamentPlayer],
    tournament_players_to_assign_room: &[TournamentPlayer],
    tournament_players_to_delete_room: &[TournamentPlayer],
    tournament_players_to_claim_coin: &[TournamentPlayer],
    tournament_players_to_delete: &[TournamentPlayer],
    rock_paper_scissors_games_to_insert: &[RockPaperScissorsGame],
    rock_paper_scissors_results_to_insert: &[RockPaperScissorsGame],
    rock_paper_scissors_players_to_insert: &[RockPaperScissorsPlayer],
    trivia_questiosn_to_insert: &[TriviaQuestion],
    trivia_answers_to_insert: &[TriviaAnswer],
    roulette: &[Roulette],
) -> Result<(), diesel::result::Error> {
    insert_tournaments(conn, tournaments_to_insert).await?;
    insert_tournament_coin_rewards(conn, tournament_coin_rewards_to_insert).await?;
    insert_tournament_token_rewards(conn, tournament_token_rewards_to_insert).await?;
    insert_tournament_rounds(conn, tournament_rounds_to_insert).await?;
    insert_tournament_rooms(conn, tournament_rooms_to_insert).await?;
    insert_tournament_rooms_to_delete(conn, tournament_rooms_to_delete).await?;
    insert_tournament_players(conn, tournament_players_to_insert).await?;
    insert_tournament_players_to_assign_room(conn, tournament_players_to_assign_room).await?;
    insert_tournament_players_to_delete_room(conn, tournament_players_to_delete_room).await?;
    insert_tournament_players_to_claim_coin(conn, tournament_players_to_claim_coin).await?;
    insert_tournament_players_to_delete(conn, tournament_players_to_delete).await?;
    insert_rock_paper_scissors_games(conn, rock_paper_scissors_games_to_insert).await?;
    insert_rock_paper_scissors_results(conn, rock_paper_scissors_results_to_insert).await?;
    insert_rock_paper_scissors_players(conn, rock_paper_scissors_players_to_insert).await?;
    insert_trivia_questions(conn, trivia_questiosn_to_insert).await?;
    insert_trivia_answers(conn, trivia_answers_to_insert).await?;
    insert_roulette(conn, roulette).await?;
    Ok(())
}

async fn insert_tournaments(
    conn: &mut MyDbConnection,
    tournaments_to_insert: &[Tournament],
) -> Result<(), diesel::result::Error> {
    use schema::tournaments::dsl::*;
    let chunks = get_chunks(tournaments_to_insert.len(), Tournament::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournaments::table)
                .values(&tournaments_to_insert[start_ind..end_ind])
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
                )),
            Some(
                " WHERE tournaments.last_transaction_version <= excluded.last_transaction_version ",
            ),
        )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_coin_rewards(
    conn: &mut MyDbConnection,
    tournament_coin_rewards_to_insert: &[TournamentCoinReward],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_coin_rewards::dsl::*;
    let chunks = get_chunks(
        tournament_coin_rewards_to_insert.len(),
        TournamentCoinReward::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_coin_rewards::table)
                .values(&tournament_coin_rewards_to_insert[start_ind..end_ind])
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
        .await?;
    }
    Ok(())
}

async fn insert_tournament_token_rewards(
    conn: &mut MyDbConnection,
    tournament_token_rewards_to_insert: &[TournamentTokenReward],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_token_rewards::dsl::*;
    let chunks = get_chunks(
        tournament_token_rewards_to_insert.len(),
        TournamentTokenReward::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_token_rewards::table)
                .values(&tournament_token_rewards_to_insert[start_ind..end_ind])
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
        .await?;
    }
    Ok(())
}

async fn insert_tournament_rounds(
    conn: &mut MyDbConnection,
    tournament_rounds_to_insert: &[TournamentRound],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_rounds::dsl::*;
    let chunks = get_chunks(
        tournament_rounds_to_insert.len(),
        TournamentRound::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_rounds::table)
                .values(&tournament_rounds_to_insert[start_ind..end_ind])
                .on_conflict(address)
                .do_update()
                .set((
                    number.eq(excluded(number)),
                    play_started.eq(excluded(play_started)),
                    play_ended.eq(excluded(play_ended)),
                    paused.eq(excluded(paused)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(
                " WHERE tournament_rounds.last_transaction_version <= excluded.last_transaction_version ",
            ),
        )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_rooms(
    conn: &mut MyDbConnection,
    tournament_rooms_to_insert: &[TournamentRoom],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_rooms::dsl::*;
    let chunks = get_chunks(
        tournament_rooms_to_insert.len(),
        TournamentRoom::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_rooms::table)
                .values(&tournament_rooms_to_insert[start_ind..end_ind])
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
            ),
        )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_rooms_to_delete(
    conn: &mut MyDbConnection,
    tournament_rooms_to_insert: &[TournamentRoom],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_rooms::dsl::*;
    let chunks = get_chunks(
        tournament_rooms_to_insert.len(),
        TournamentRoom::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_rooms::table)
                .values(&tournament_rooms_to_insert[start_ind..end_ind])
                .on_conflict(address)
                .do_update()
                .set((
                    in_progress.eq(excluded(in_progress)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
            Some(
                " WHERE tournament_rooms.last_transaction_version <= excluded.last_transaction_version ",
            ),
        )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_players(
    conn: &mut MyDbConnection,
    tournament_players_to_insert: &[TournamentPlayer],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_players::dsl::*;
    let chunks = get_chunks(
        tournament_players_to_insert.len(),
        TournamentPlayer::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_players::table)
                .values(&tournament_players_to_insert[start_ind..end_ind])
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
                ),
            )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_players_to_assign_room(
    conn: &mut MyDbConnection,
    tournament_players_to_insert: &[TournamentPlayer],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_players::dsl::*;
    let chunks = get_chunks(
        tournament_players_to_insert.len(),
        TournamentPlayer::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_players::table)
                .values(&tournament_players_to_insert[start_ind..end_ind])
                .on_conflict(token_address)
                .do_update()
                .set((
                    room_address.eq(excluded(room_address)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ),
            )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_players_to_delete_room(
    conn: &mut MyDbConnection,
    tournament_players_to_insert: &[TournamentPlayer],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_players::dsl::*;
    let chunks = get_chunks(
        tournament_players_to_insert.len(),
        TournamentPlayer::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_players::table)
                .values(&tournament_players_to_insert[start_ind..end_ind])
                .on_conflict(token_address)
                .do_update()
                .set((
                    room_address.eq(excluded(room_address)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ),
            )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_players_to_claim_coin(
    conn: &mut MyDbConnection,
    tournament_players_to_insert: &[TournamentPlayer],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_players::dsl::*;
    let chunks = get_chunks(
        tournament_players_to_insert.len(),
        TournamentPlayer::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_players::table)
                .values(&tournament_players_to_insert[start_ind..end_ind])
                .on_conflict(token_address)
                .do_update()
                .set((
                    coin_reward_claimed_type.eq(excluded(coin_reward_claimed_type)),
                    coin_reward_claimed_amount.eq(excluded(coin_reward_claimed_amount)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ),
            )
        .await?;
    }
    Ok(())
}

async fn insert_tournament_players_to_delete(
    conn: &mut MyDbConnection,
    tournament_players_to_insert: &[TournamentPlayer],
) -> Result<(), diesel::result::Error> {
    use schema::tournament_players::dsl::*;
    let chunks = get_chunks(
        tournament_players_to_insert.len(),
        TournamentPlayer::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tournament_players::table)
                .values(&tournament_players_to_insert[start_ind..end_ind])
                .on_conflict(token_address)
                .do_update()
                .set((
                    alive.eq(excluded(alive)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE tournament_players.last_transaction_version <= excluded.last_transaction_version ",
                ),
            )
        .await?;
    }
    Ok(())
}

async fn insert_rock_paper_scissors_games(
    conn: &mut MyDbConnection,
    rock_paper_scissors_games_to_insert: &[RockPaperScissorsGame],
) -> Result<(), diesel::result::Error> {
    use schema::rock_paper_scissors_games::dsl::*;
    let chunks = get_chunks(
        rock_paper_scissors_games_to_insert.len(),
        RockPaperScissorsGame::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::rock_paper_scissors_games::table)
                .values(&rock_paper_scissors_games_to_insert[start_ind..end_ind])
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
                ),
            )
        .await?;
    }
    Ok(())
}
async fn insert_rock_paper_scissors_results(
    conn: &mut MyDbConnection,
    rock_paper_scissors_games_to_insert: &[RockPaperScissorsGame],
) -> Result<(), diesel::result::Error> {
    use schema::rock_paper_scissors_games::dsl::*;
    let chunks = get_chunks(
        rock_paper_scissors_games_to_insert.len(),
        RockPaperScissorsGame::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::rock_paper_scissors_games::table)
                .values(&rock_paper_scissors_games_to_insert[start_ind..end_ind])
                .on_conflict(room_address)
                .do_update()
                .set((
                    winners.eq(excluded(winners)),
                    losers.eq(excluded(losers)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
                Some(
                    " WHERE rock_paper_scissors_games.last_transaction_version <= excluded.last_transaction_version ",
                ),
            )
        .await?;
    }
    Ok(())
}

async fn insert_rock_paper_scissors_players(
    conn: &mut MyDbConnection,
    rock_paper_scissors_players_to_insert: &[RockPaperScissorsPlayer],
) -> Result<(), diesel::result::Error> {
    use schema::rock_paper_scissors_players::dsl::*;
    let chunks = get_chunks(
        rock_paper_scissors_players_to_insert.len(),
        RockPaperScissorsPlayer::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::rock_paper_scissors_players::table)
                .values(&rock_paper_scissors_players_to_insert[start_ind..end_ind])
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
        .await?;
    }
    Ok(())
}

async fn insert_trivia_questions(
    conn: &mut MyDbConnection,
    trivia_questiosn_to_insert: &[TriviaQuestion],
) -> Result<(), diesel::result::Error> {
    use schema::trivia_questions::dsl::*;
    let chunks = get_chunks(
        trivia_questiosn_to_insert.len(),
        TriviaQuestion::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::trivia_questions::table)
                .values(&trivia_questiosn_to_insert[start_ind..end_ind])
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
        .await?;
    }
    Ok(())
}

async fn insert_trivia_answers(
    conn: &mut MyDbConnection,
    trivia_answers_to_insert: &[TriviaAnswer],
) -> Result<(), diesel::result::Error> {
    use schema::trivia_answers::dsl::*;
    let chunks = get_chunks(trivia_answers_to_insert.len(), TriviaAnswer::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::trivia_answers::table)
                .values(&trivia_answers_to_insert[start_ind..end_ind])
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
        .await?;
    }
    Ok(())
}

async fn insert_roulette(
    conn: &mut MyDbConnection,
    items_to_insert: &[Roulette],
) -> Result<(), diesel::result::Error> {
    use schema::roulette::dsl::*;
    let chunks = get_chunks(items_to_insert.len(), Roulette::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::roulette::table)
                .values(&items_to_insert[start_ind..end_ind])
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
        .await?;
    }
    Ok(())
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
        let conn = &mut self.connection_pool.get().await?;
        let processing_start = std::time::Instant::now();

        let mut tournament_token_reward_claims = HashMap::new();

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
                        create_room_events.insert(
                            standardize_address(
                                &event.key.clone().unwrap().account_address.as_ref(),
                            ),
                            create_room,
                        );
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
                        conn,
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

        let mut tournaments = tournaments.values().cloned().collect::<Vec<_>>();
        let mut tournament_coin_rewards = tournament_coin_rewards
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut tournament_token_rewards = tournament_token_rewards
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut tournament_rounds = tournament_rounds.values().cloned().collect::<Vec<_>>();
        let mut tournament_rooms = tournament_rooms.values().cloned().collect::<Vec<_>>();
        let mut tournament_rooms_to_delete = tournament_rooms_to_delete
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut tournament_players = tournament_players.values().cloned().collect::<Vec<_>>();
        let mut tournament_players_to_assign_room = tournament_players_to_assign_room
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut tournament_players_to_delete_room = tournament_players_to_delete_room
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut tournament_players_to_claim_coin = tournament_players_to_claim_coin
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut tournament_players_to_delete = tournament_players_to_delete
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut rock_paper_scissors_games = rock_paper_scissors_games
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut rock_paper_scissors_results = rock_paper_scissors_results
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut rock_paper_scissors_players = rock_paper_scissors_players
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut trivia_questions = trivia_questions.values().cloned().collect::<Vec<_>>();
        let mut trivia_answers = trivia_answers.values().cloned().collect::<Vec<_>>();
        let mut roulette = roulette.values().cloned().collect::<Vec<_>>();

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
            conn,
            self.name(),
            start_version,
            end_version,
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
