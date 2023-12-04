// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]
use crate::{
    schema::processor_status,
    utils::database::{execute_with_better_error, PgPoolConnection},
};
use diesel::{upsert::excluded, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;

#[derive(AsChangeset, Debug, Insertable)]
#[diesel(table_name = processor_status)]
/// Only tracking the latest version successfully processed
pub struct ProcessorStatus {
    pub processor: String,
    pub last_success_version: i64,
}

#[derive(AsChangeset, Debug, Queryable)]
#[diesel(table_name = processor_status)]
/// Only tracking the latest version successfully processed
pub struct ProcessorStatusQuery {
    pub processor: String,
    pub last_success_version: i64,
    pub last_updated: chrono::NaiveDateTime,
}

impl ProcessorStatusQuery {
    pub async fn get_by_processor(
        processor_name: &str,
        conn: &mut PgPoolConnection<'_>,
    ) -> diesel::QueryResult<Option<Self>> {
        processor_status::table
            .filter(processor_status::processor.eq(processor_name))
            .first::<Self>(conn)
            .await
            .optional()
    }
}

/// Store last processed version from database. We can assume that all previously
/// processed versions are successful because any gap would cause the processor to
/// panic.
pub async fn update_last_processed_version(
    mut conn: PgPoolConnection<'_>,
    processor_name: String,
    version: u64,
) -> anyhow::Result<()> {
    let status = ProcessorStatus {
        processor: processor_name,
        last_success_version: version as i64,
    };
    execute_with_better_error(
        &mut conn,
        diesel::insert_into(processor_status::table)
            .values(&status)
            .on_conflict(processor_status::processor)
            .do_update()
            .set((
                processor_status::last_success_version
                    .eq(excluded(processor_status::last_success_version)),
                processor_status::last_updated.eq(excluded(processor_status::last_updated)),
            )),
        Some(" WHERE processor_status.last_success_version <= EXCLUDED.last_success_version "),
    )
    .await?;
    Ok(())
}
