// Copyright © Aptos Foundation

use crate::{
    aptos_tournament_schema::aptos_tournament as schema,
    aptos_tournament_schema::aptos_tournament::rooms,
    utils::database::{execute_with_better_error, MyDbConnection, PgPoolConnection},
};
use anyhow::Context;
use diesel::{prelude::*, result::Error, upsert::excluded};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(round_address, address))]
#[diesel(table_name = rooms)]
pub struct Room {
    pub round_address: String,
    pub address: String,
    pub players_per_room: Option<i32>,
}

impl Room {
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
        round_address_pk: String,
        address_pk: String,
    ) -> anyhow::Result<Option<Self>> {
        rooms::table
            .find((round_address_pk, address_pk))
            .first::<Self>(conn)
            .await
            .optional()
            .context("querying rooms")
    }

    async fn upsert_impl(&self, conn: &mut MyDbConnection) -> Result<usize, diesel::result::Error> {
        use schema::rooms::dsl::*;

        let query = diesel::insert_into(schema::rooms::table)
            .values(self)
            .on_conflict((round_address, address))
            .do_update()
            .set((
                round_address.eq(excluded(round_address)),
                address.eq(excluded(address)),
                players_per_room.eq(excluded(players_per_room)),
            ));

        let debug_query = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
        debug!("Executing Query: {}", debug_query);
        execute_with_better_error(conn, query, None).await
    }
}
