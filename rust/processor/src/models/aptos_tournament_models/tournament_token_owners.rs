// Copyright © Aptos Foundation

use crate::{schema::tournament_token_owners, utils::database::PgPoolConnection};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_address))]
#[diesel(table_name = tournament_token_owners)]
pub struct TournamentTokenOwner {
    pub token_address: String,
    pub user_address: String,
    pub tournament_address: String,
}

impl TournamentTokenOwner {
    pub fn new(
        token_address: String,
        user_address: String,
        tournament_address: String,
    ) -> TournamentTokenOwner {
        TournamentTokenOwner {
            token_address,
            user_address,
            tournament_address,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Identifiable, Queryable, Serialize)]
#[diesel(primary_key(token_address))]
#[diesel(table_name = tournament_token_owners)]
pub struct TournamentTokenOwnerQuery {
    pub token_address: String,
    pub user_address: String,
    pub tournament_address: String,
    pub inserted_at: chrono::NaiveDateTime,
}

impl TournamentTokenOwnerQuery {
    pub async fn query(conn: &mut PgPoolConnection<'_>, token_address: &str) -> Option<Self> {
        tournament_token_owners::table
            .find(token_address)
            .first::<TournamentTokenOwnerQuery>(conn)
            .await
            .optional()
            .unwrap_or_else(|e| {
                error!(
                    token_address = token_address,
                    error = ?e,
                    "Error querying tournament token owner"
                );
                panic!();
            })
    }
}
