// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{schema::ledger_infos, utils::database::DbPoolConnection};
use diesel::{Identifiable, Insertable, OptionalExtension, QueryDsl, Queryable};
use diesel_async::RunQueryDsl;

#[derive(Debug, Identifiable, Insertable, Queryable)]
#[diesel(table_name = ledger_infos)]
#[diesel(primary_key(chain_id))]
pub struct LedgerInfo {
    pub chain_id: i64,
}

impl LedgerInfo {
    pub async fn get(conn: &mut DbPoolConnection<'_>) -> diesel::QueryResult<Option<Self>> {
        ledger_infos::table
            .select(ledger_infos::all_columns)
            .first::<Self>(conn)
            .await
            .optional()
    }
}
