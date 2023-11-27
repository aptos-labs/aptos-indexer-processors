// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
        aptos_tournament_models::{
            aptos_tournament_utils::{
                CurrentRound, CurrentRoundMapping, TournamentState, TournamentStateMapping,
            },
            tournament_players::{TournamentPlayer, TournamentPlayerMapping},
            tournament_rounds::{TournamentRound, TournamentRoundsMapping},
            tournaments::{Tournament, TournamentMapping},
        },
        token_v2_models::v2_token_utils::ObjectWithMetadata,
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
    tournament_rounds_to_insert: Vec<TournamentRound>,
    tournament_players_to_insert: Vec<TournamentPlayer>,
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
                &tournament_rounds_to_insert,
                &tournament_players_to_insert,
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
                        let tournament_rounds_to_insert =
                            clean_data_for_db(tournament_rounds_to_insert, true);
                        let tournament_players_to_insert =
                            clean_data_for_db(tournament_players_to_insert, true);
                        insert_to_db_impl(
                            pg_conn,
                            &tournaments_to_insert,
                            &tournament_rounds_to_insert,
                            &tournament_players_to_insert,
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
    tournament_rounds_to_insert: &[TournamentRound],
    tournament_players_to_insert: &[TournamentPlayer],
) -> Result<(), diesel::result::Error> {
    insert_tournaments(conn, tournaments_to_insert).await?;
    insert_tournament_rounds(conn, tournament_rounds_to_insert).await?;
    insert_tournament_players(conn, tournament_players_to_insert).await?;
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
                    has_ended.eq(excluded(has_ended)),
                    current_round_address.eq(excluded(current_round_address)),
                    current_round_number.eq(excluded(current_round_number)),
                    current_game_module.eq(excluded(current_game_module)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
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
                    room_address.eq(excluded(room_address)),
                    alive.eq(excluded(alive)),
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

        let mut tournaments: TournamentMapping = HashMap::new();
        let mut tournament_rounds: TournamentRoundsMapping = HashMap::new();
        let mut tournament_players: TournamentPlayerMapping = HashMap::new();

        for txn in transactions {
            let mut tournament_state_mapping: TournamentStateMapping = HashMap::new();
            let mut current_round_mapping: CurrentRoundMapping = HashMap::new();
            let mut object_to_owner = HashMap::new();

            let txn_version = txn.version as i64;
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");

            if let TxnData::User(user_txn) = txn_data {
                // First pass: TournamentState and CurrentRound
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
                    }
                }

                // Second pass: get tournament players metadata
                for wsc in transaction_info.changes.iter() {
                    if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                        let address = standardize_address(&wr.address.to_string());
                        if let Some(tournament_player) = TournamentPlayer::from_tournament_token(
                            conn,
                            &self.config.contract_address,
                            wr,
                            txn_version,
                            &object_to_owner,
                            &tournament_players,
                        )
                        .await
                        {
                            tournament_players.insert(address.clone(), tournament_player);
                        }
                    }
                }

                // Third pass: everything else
                for wsc in transaction_info.changes.iter() {
                    match wsc.change.as_ref().unwrap() {
                        Change::WriteResource(wr) => {
                            let address = standardize_address(&wr.address.to_string());
                            if let Some(tournament) = Tournament::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                                tournament_state_mapping.clone(),
                                current_round_mapping.clone(),
                            ) {
                                tournaments.insert(address.clone(), tournament);
                            }
                            if let Some(round) = TournamentRound::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                            ) {
                                tournament_rounds.insert(address.clone(), round);
                            }

                            let players = TournamentPlayer::from_room(
                                conn,
                                &self.config.contract_address,
                                wr,
                                txn_version,
                                &tournament_players,
                            )
                            .await;
                            tournament_players.extend(players);
                        },
                        _ => {},
                    }
                }

                // Pass through events for player and room burning
                for event in user_txn.events.iter() {
                    if let Some(player) = TournamentPlayer::delete_player(
                        conn,
                        &self.config.contract_address,
                        event,
                        txn_version,
                        &tournament_players,
                    )
                    .await
                    {
                        tournament_players.insert(player.token_address.clone(), player);
                    }

                    let players = TournamentPlayer::delete_room(
                        conn,
                        &self.config.contract_address,
                        event,
                        txn_version,
                    )
                    .await;
                    tournament_players.extend(players);
                }
            }
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let mut tournaments = tournaments.values().cloned().collect::<Vec<_>>();
        let mut tournament_rounds = tournament_rounds.values().cloned().collect::<Vec<_>>();
        let mut tournament_players = tournament_players.values().cloned().collect::<Vec<_>>();

        tournaments.sort();
        tournament_rounds.sort();
        tournament_players.sort();

        insert_to_db(
            conn,
            self.name(),
            start_version,
            end_version,
            tournaments,
            tournament_rounds,
            tournament_players,
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
