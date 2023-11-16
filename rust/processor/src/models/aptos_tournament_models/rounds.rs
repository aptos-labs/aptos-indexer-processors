// Copyright © Aptos Foundation

use crate::{
    schema::{self, rounds},
    utils::database::{execute_with_better_error, MyDbConnection, PgPoolConnection},
};
use anyhow::Context;
use diesel::{prelude::*, result::Error, upsert::excluded};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(tournament_address, number))]
#[diesel(table_name = rounds)]
pub struct Round {
    pub tournament_address: String,
    pub number: i32,
    pub address: String,
    pub game_module: String,
    pub matchmaking_ended: bool,
    pub play_started: bool,
    pub play_ended: bool,
    pub paused: bool,
    pub matchmaker_address: String,
}

impl Round {
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
        tournament_address_pk: String,
        number_pk: i32,
    ) -> anyhow::Result<Option<Self>> {
        rounds::table
            .find((tournament_address_pk, number_pk))
            .first::<Self>(conn)
            .await
            .optional()
            .context("querying rounds")
    }

    async fn upsert_impl(&self, conn: &mut MyDbConnection) -> Result<usize, diesel::result::Error> {
        use schema::rounds::dsl::*;

        let query = diesel::insert_into(schema::rounds::table)
            .values(self)
            .on_conflict((tournament_address, number))
            .do_update()
            .set((
                address.eq(excluded(address)),
                game_module.eq(excluded(game_module)),
                matchmaking_ended.eq(excluded(matchmaking_ended)),
                play_started.eq(excluded(play_started)),
                play_ended.eq(excluded(play_ended)),
                paused.eq(excluded(paused)),
                matchmaker_address.eq(excluded(matchmaker_address)),
            ));

        let debug_query = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
        debug!("Executing Query: {}", debug_query);
        execute_with_better_error(conn, query, None).await
    }
}
