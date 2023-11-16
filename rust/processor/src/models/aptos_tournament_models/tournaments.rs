// Copyright © Aptos Foundation

use crate::{
    aptos_tournament_schema::aptos_tournament as schema,
    aptos_tournament_schema::aptos_tournament::tournaments,
    utils::database::{execute_with_better_error, MyDbConnection, PgPoolConnection},
};
use anyhow::Context;
use diesel::{prelude::*, result::Error, upsert::excluded};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(address))]
#[diesel(table_name = tournaments)]
pub struct Tournament {
    pub address: String,
    pub tournament_name: String,
    pub max_players: String,
    pub max_num_winners: i32,
    pub players_joined: i32,
    pub secondary_admin_address: Option<String>,
    pub is_joinable: bool,
    pub has_ended: bool,
}

impl Tournament {
    pub async fn upsert(
        &self,
        conn: &mut PgPoolConnection<'_>,
    ) -> Result<usize, diesel::result::Error> {
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| Box::pin(self.upsert_impl(pg_conn)))
            .await
    }

    pub async fn query(
        conn: &mut PgPoolConnection<'_>,
        address_pk: String,
    ) -> anyhow::Result<Option<Self>> {
        tournaments::table
            .find(address_pk)
            .first::<Self>(conn)
            .await
            .optional()
            .context("querying tournaments")
    }

    async fn upsert_impl(&self, conn: &mut MyDbConnection) -> Result<usize, diesel::result::Error> {
        use schema::tournaments::dsl::*;

        let query = diesel::insert_into(schema::tournaments::table)
            .values(self)
            .on_conflict(address)
            .do_update()
            .set((
                address.eq(excluded(address)),
                tournament_name.eq(excluded(tournament_name)),
                max_players.eq(excluded(max_players)),
                max_num_winners.eq(excluded(max_num_winners)),
                players_joined.eq(excluded(players_joined)),
                secondary_admin_address.eq(excluded(secondary_admin_address)),
                is_joinable.eq(excluded(is_joinable)),
                has_ended.eq(excluded(has_ended)),
            ));

        let debug_query = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
        debug!("Executing Query: {}", debug_query);
        execute_with_better_error(conn, query, None).await
    }
}
