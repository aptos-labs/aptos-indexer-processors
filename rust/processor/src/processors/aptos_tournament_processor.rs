// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::aptos_tournament_models::{
        aptos_tournament_utils::{CurrentRound, TournamentState},
        tournament_players::TournamentPlayer,
        tournament_rooms::TournamentRoom,
        tournament_rounds::TournamentRound,
        tournaments::Tournament,
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
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};
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

        // TODO: Process transactions
        // I'm probably missing the DB lookup thing that you mentioned if the required transaction is outside of the batch
        // Also need to find out how to handle the nft that represents u can play
        let mut tournament_state_mapping = HashMap::new();
        let mut current_round_mapping = HashMap::new();

        let mut tournaments: HashMap<String, (i64, Tournament)> = HashMap::new();
        let mut tournament_rounds: HashMap<String, (i64, TournamentRound)> = HashMap::new();
        let mut tournament_rooms: HashMap<String, (i64, TournamentRoom)> = HashMap::new();
        let mut tournament_players: HashMap<String, (i64, TournamentPlayer)> = HashMap::new();
        for txn in transactions {
            let txn_version = txn.version as i64;
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

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
                }
            }

            // Second pass: everything else
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());
                    if let Some(tournament) = Tournament::from_write_resource(
                        &self.config.contract_address,
                        wr,
                        txn_version,
                        tournament_state_mapping.clone(),
                        current_round_mapping.clone(),
                    ) {
                        tournaments.insert(address.clone(), (txn_version, tournament));
                    }
                    if let Some(round) = TournamentRound::from_write_resource(
                        &self.config.contract_address,
                        wr,
                        txn_version,
                    ) {
                        tournament_rounds.insert(address.clone(), (txn_version, round));
                    }
                }
            }
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let mut tournaments = tournaments.values().cloned().collect::<Vec<_>>();
        let mut tournament_rounds = tournament_rounds.values().cloned().collect::<Vec<_>>();
        let mut tournament_rooms = tournament_rooms.values().cloned().collect::<Vec<_>>();
        let mut tournament_players = tournament_players.values().cloned().collect::<Vec<_>>();

        tournaments.sort_by(|a, b| a.0.cmp(&b.0));
        tournament_rounds.sort_by(|a, b| a.0.cmp(&b.0));
        tournament_rooms.sort_by(|a, b| a.0.cmp(&b.0));
        tournament_players.sort_by(|a, b| a.0.cmp(&b.0));

        insert_to_db(
            &mut self.connection_pool.get().await?,
            self.name(),
            start_version,
            end_version,
            tournaments.into_iter().map(|(_, s)| s).collect(),
            tournament_rounds.into_iter().map(|(_, s)| s).collect(),
            tournament_rooms.into_iter().map(|(_, s)| s).collect(),
            tournament_players.into_iter().map(|(_, s)| s).collect(),
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

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    tournaments_to_insert: Vec<Tournament>,
    tournament_rounds_to_insert: Vec<TournamentRound>,
    tournament_rooms_to_insert: Vec<TournamentRoom>,
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
                &tournament_rooms_to_insert,
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
                        let tournament_rooms_to_insert =
                            clean_data_for_db(tournament_rooms_to_insert, true);
                        let tournament_players_to_insert =
                            clean_data_for_db(tournament_players_to_insert, true);
                        insert_to_db_impl(
                            pg_conn,
                            &tournaments_to_insert,
                            &tournament_rounds_to_insert,
                            &tournament_rooms_to_insert,
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
    tournament_rooms_to_insert: &[TournamentRoom],
    tournament_players_to_insert: &[TournamentPlayer],
) -> Result<(), diesel::result::Error> {
    insert_tournaments(conn, tournaments_to_insert).await?;
    insert_tournament_rounds(conn, tournament_rounds_to_insert).await?;
    insert_tournament_rooms(conn, tournament_rooms_to_insert).await?;
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
                )),
            None,
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
                    matchmaking_ended.eq(excluded(matchmaking_ended)),
                    play_started.eq(excluded(play_started)),
                    play_ended.eq(excluded(play_ended)),
                    paused.eq(excluded(paused)),
                    matchmaker_address.eq(excluded(matchmaker_address)),
                )),
            None,
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
                .on_conflict((round_address, address))
                .do_update()
                .set((players_per_room.eq(excluded(players_per_room)),)),
            None,
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
                .on_conflict((address, tournament_address))
                .do_update()
                .set((
                    room_address.eq(excluded(room_address)),
                    token_address.eq(excluded(token_address)),
                    alive.eq(excluded(alive)),
                    submitted.eq(excluded(submitted)),
                )),
            None,
        )
        .await?;
    }
    Ok(())
}
