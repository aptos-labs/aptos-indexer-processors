// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
        aptos_tournament_models::{
            aptos_tournament_utils::{
                AptosTournament, AptosTournamentAggregatedData, CurrentRound, GameOverEvent,
                RPSGame, TournamentDirector, TournamentState, TournamentToken,
            },
            players::Player,
            rooms::Room,
            rounds::Round,
            tournaments::Tournament,
        },
        token_v2_models::v2_token_utils::ObjectWithMetadata,
    },
    utils::{database::PgDbPool, util::standardize_address},
};
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
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
        let mut aptos_tournament_metadata_helper = HashMap::new();
        let mut tournaments: HashMap<String, Tournament> = HashMap::new();
        let mut rounds: HashMap<(String, i32), Round> = HashMap::new();
        let mut rooms: HashMap<(String, String), Room> = HashMap::new();
        let mut players: HashMap<(String, String), Player> = HashMap::new();
        for txn in transactions {
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            let txn_version = txn.version as i64;
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

            if let TxnData::User(user_txn) = txn_data {
                let user_request = user_txn
                    .request
                    .as_ref()
                    .expect("Sends is not present in user txn");

                // First pass: create mappings {obj_addr: obj}
                for wsc in transaction_info.changes.iter() {
                    if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                        if let Some(object) =
                            ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                        {
                            aptos_tournament_metadata_helper.insert(
                                standardize_address(&wr.address.to_string()),
                                AptosTournamentAggregatedData {
                                    aptos_tournament: None,
                                    current_round: None,
                                    // refs: None,
                                    rps_game: None,
                                    tournament_director: None,
                                    tournament_state: None,
                                    tournament_token: None,
                                    object,
                                },
                            );
                        }
                    }
                }

                // Second pass: loop through write sets to get all the structs related to the object
                // let mut aptos_tournament_metadata_helper = HashMap::new();
                for wsc in transaction_info.changes.iter() {
                    if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                        let address = standardize_address(&wr.address.to_string());
                        if let Some(aggregated_data) =
                            aptos_tournament_metadata_helper.get_mut(&address)
                        {
                            if let Some(aptos_tournament) = AptosTournament::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                            )? {
                                aggregated_data.aptos_tournament = Some(aptos_tournament);
                            }
                            if let Some(current_round) = CurrentRound::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                            )? {
                                aggregated_data.current_round = Some(current_round);
                            }
                            // if let Some(refs) = Refs::from_write_resource(
                            //     &self.config.contract_address,
                            //     wr,
                            //     txn_version,
                            // )? {
                            //     aggregated_data.refs = Some(refs);
                            // }
                            if let Some(rps_game) = RPSGame::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                            )? {
                                aggregated_data.rps_game = Some(rps_game);
                            }
                            if let Some(tournament_director) =
                                TournamentDirector::from_write_resource(
                                    &self.config.contract_address,
                                    wr,
                                    txn_version,
                                )?
                            {
                                aggregated_data.tournament_director = Some(tournament_director);
                            }
                            if let Some(tournament_state) = TournamentState::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                            )? {
                                aggregated_data.tournament_state = Some(tournament_state);
                            }
                            if let Some(tournament_token) = TournamentToken::from_write_resource(
                                &self.config.contract_address,
                                wr,
                                txn_version,
                            )? {
                                aggregated_data.tournament_token = Some(tournament_token);
                            }
                        }
                    }
                }

                // Third pass through events for end game event (probably need this to end the game/set winner/something )
                for wsc in user_txn.events.iter() {
                    if let Some(game_over_event) =
                        GameOverEvent::from_event(&self.config.contract_address, wsc, txn_version)?
                    {
                        // TODO: Handle this
                    }
                }

                // Fourth pass to collect all the data
                for wsc in transaction_info.changes.iter() {
                    if let Some(Change::WriteResource(resource)) = wsc.change.as_ref() {
                        // TODO: Handle collection
                    }
                }
            }
        }
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        // TODO: write to db
        println!(
            "aptos_tournament_metadata_helper: {:?}",
            aptos_tournament_metadata_helper
        );

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
