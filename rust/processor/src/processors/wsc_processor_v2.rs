// wsc_processor_v2.rs
// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    entity::{
        write_set_changes::{ActiveModel as WriteSetChangesActiveModel, WriteSetChangeEnum},
        write_set_changes_module::{self},
        write_set_changes_resource::{self},
        write_set_changes_table::{self},
    },
    models::default_models::transactions::TransactionModel,
    utils::database::PgDbPool,
};
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction as TransactionPB;
use async_trait::async_trait;
use migration::{
    sea_orm::{DatabaseConnection, EntityTrait},
    sea_query,
};
use std::fmt::Debug;
use tracing::error;

pub struct WscProcessorV2 {
    connection_pool: PgDbPool,
    cockroach_connection_pool: DatabaseConnection,
}

impl WscProcessorV2 {
    pub fn new(connection_pool: PgDbPool, cockroach_connection_pool: DatabaseConnection) -> Self {
        tracing::info!("init WscProcessorV2");
        Self {
            connection_pool,
            cockroach_connection_pool,
        }
    }
}

impl Debug for WscProcessorV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "WscProcessorV2 {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for WscProcessorV2 {
    fn name(&self) -> &'static str {
        ProcessorName::WscProcessorV2.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<TransactionPB>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();

        let (_, _, write_set_changes, wsc_details) =
            TransactionModel::from_transactions(&transactions);
        let wscs = WriteSetChangesActiveModel::from_wscs(write_set_changes, wsc_details);
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();
        let mut modules = Vec::new();
        let mut resources = Vec::new();
        let mut tables = Vec::new();

        for wsc in wscs {
            match wsc {
                WriteSetChangeEnum::Module(module) => modules.push(module),
                WriteSetChangeEnum::Resource(resource) => resources.push(resource),
                WriteSetChangeEnum::Table(table) => tables.push(table),
            }
        }

        if !modules.is_empty() {
            if let Err(err) = write_set_changes_module::Entity::insert_many(modules)
                .on_conflict(
                    sea_query::OnConflict::columns([
                        write_set_changes_module::Column::TransactionVersion,
                        write_set_changes_module::Column::Index,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .do_nothing()
                .exec(&self.cockroach_connection_pool.clone())
                .await
            {
                let _ = log_and_bail(
                    start_version,
                    end_version,
                    self.name(),
                    "write set changes",
                    err,
                );
            }
        }

        if !resources.is_empty() {
            if let Err(err) = write_set_changes_resource::Entity::insert_many(resources)
                .on_conflict(
                    sea_query::OnConflict::columns([
                        write_set_changes_resource::Column::TransactionVersion,
                        write_set_changes_resource::Column::Index,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .do_nothing()
                .exec(&self.cockroach_connection_pool.clone())
                .await
            {
                let _ = log_and_bail(
                    start_version,
                    end_version,
                    self.name(),
                    "write set changes",
                    err,
                );
            }
        }

        if !tables.is_empty() {
            if let Err(err) = write_set_changes_table::Entity::insert_many(tables)
                .on_conflict(
                    sea_query::OnConflict::columns([
                        write_set_changes_table::Column::TransactionVersion,
                        write_set_changes_table::Column::Index,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .do_nothing()
                .exec(&self.cockroach_connection_pool.clone())
                .await
            {
                let _ = log_and_bail(
                    start_version,
                    end_version,
                    self.name(),
                    "write set changes",
                    err,
                );
            }
        }
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

fn log_and_bail(
    start_version: u64,
    end_version: u64,
    processor_name: &str,
    entity_name: &str,
    err: migration::DbErr,
) -> anyhow::Result<()> {
    error!(
        start_version = start_version,
        end_version = end_version,
        processor_name = processor_name,
        "[Parser] Error inserting {} to db: {:?}",
        entity_name,
        err
    );

    bail!(format!(
        "Error inserting {} to db. Processor {}. Start {}. End {}. Error {:?}",
        entity_name, processor_name, start_version, end_version, err
    ));
}
