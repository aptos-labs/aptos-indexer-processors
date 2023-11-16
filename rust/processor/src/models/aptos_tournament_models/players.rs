// Copyright © Aptos Foundation

use crate::{
    aptos_tournament_schema::aptos_tournament as schema,
    aptos_tournament_schema::aptos_tournament::players,
    utils::database::{execute_with_better_error, MyDbConnection, PgPoolConnection},
};
use anyhow::Context;
use diesel::{prelude::*, result::Error, upsert::excluded};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(address, tournament_address))]
#[diesel(table_name = players)]
pub struct Player {
    pub address: String,
    pub tournament_address: String,
    pub room_address: Option<String>,
    pub token_address: String,
    pub alive: bool,
    pub submitted: bool,
}

impl Player {
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
        tournament_address_pk: String,
    ) -> anyhow::Result<Option<Self>> {
        players::table
            .find((address_pk, tournament_address_pk))
            .first::<Self>(conn)
            .await
            .optional()
            .context("querying players")
    }

    async fn upsert_impl(&self, conn: &mut MyDbConnection) -> Result<usize, diesel::result::Error> {
        use schema::players::dsl::*;

        let query = diesel::insert_into(schema::players::table)
            .values(self)
            .on_conflict((address, room_address))
            .do_update()
            .set((
                token_address.eq(excluded(token_address)),
                alive.eq(excluded(alive)),
                submitted.eq(excluded(submitted)),
            ));

        let debug_query = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
        debug!("Executing Query: {}", debug_query);
        execute_with_better_error(conn, query, None).await
    }
}
