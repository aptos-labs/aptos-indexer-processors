// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Database-related functions
#![allow(clippy::extra_unused_lifetimes)]
use crate::utils::util::remove_null_bytes;
use diesel::{
    pg::Pg,
    query_builder::{AstPass, Query, QueryFragment},
    QueryResult,
};
use diesel_async::{
    pg::AsyncPgConnection,
    pooled_connection::{
        bb8::{Pool, PooledConnection},
        AsyncDieselConnectionManager, PoolError,
    },
    RunQueryDsl,
};
use diesel_async_migrations::{embed_migrations, EmbeddedMigrations};
use migration::sea_orm::{Database, DatabaseConnection, DbErr};
use once_cell::sync::Lazy;
use std::{cmp::min, sync::Arc};

pub type MyDbConnection = AsyncPgConnection;
pub type PgPool = Pool<MyDbConnection>;
pub type PgDbPool = Arc<PgPool>;
pub type PgPoolConnection<'a> = PooledConnection<'a, MyDbConnection>;

pub static MIGRATIONS: Lazy<EmbeddedMigrations> = Lazy::new(|| embed_migrations!());

#[derive(QueryId)]
/// Using this will append a where clause at the end of the string upsert function, e.g.
/// INSERT INTO ... ON CONFLICT DO UPDATE SET ... WHERE "transaction_version" = excluded."transaction_version"
/// This is needed when we want to maintain a table with only the latest state
pub struct UpsertFilterLatestTransactionQuery<T> {
    query: T,
    where_clause: Option<&'static str>,
}

// the max is actually u16::MAX but we see that when the size is too big we get an overflow error so reducing it a bit
pub const MAX_DIESEL_PARAM_SIZE: u16 = u16::MAX / 2;

/// Given diesel has a limit of how many parameters can be inserted in a single operation (u16::MAX)
/// we may need to chunk an array of items based on how many columns are in the table.
/// This function returns boundaries of chunks in the form of (start_index, end_index)
pub fn get_chunks(num_items_to_insert: usize, column_count: usize) -> Vec<(usize, usize)> {
    let max_item_size = MAX_DIESEL_PARAM_SIZE as usize / column_count;
    let mut chunk: (usize, usize) = (0, min(num_items_to_insert, max_item_size));
    let mut chunks = vec![chunk];
    while chunk.1 != num_items_to_insert {
        chunk = (
            chunk.0 + max_item_size,
            min(num_items_to_insert, chunk.1 + max_item_size),
        );
        chunks.push(chunk);
    }
    chunks
}

/// This function will clean the data for postgres. Currently it has support for removing
/// null bytes from strings but in the future we will add more functionality.
pub fn clean_data_for_db<T: serde::Serialize + for<'de> serde::Deserialize<'de>>(
    items: Vec<T>,
    should_remove_null_bytes: bool,
) -> Vec<T> {
    if should_remove_null_bytes {
        items.iter().map(remove_null_bytes).collect()
    } else {
        items
    }
}

pub async fn new_db_pool(database_url: &str) -> Result<PgDbPool, PoolError> {
    let config = AsyncDieselConnectionManager::<MyDbConnection>::new(database_url);
    Ok(Arc::new(Pool::builder().build(config).await?))
}

pub async fn new_db_pool_v2(database_url: &str) -> Result<DatabaseConnection, DbErr> {
    let db: DatabaseConnection = Arc::new(Database::connect(database_url).await.as_ref())
        .expect("Failed to setup the database for db pool v2.")
        .clone();

    Ok(db.clone())
}

/*
pub async fn get_connection(pool: &PgPool) -> Result<AsyncConnectionWrapper<AsyncPgConnection>, PoolError> {
    let connection = pool.get().await.unwrap();
    Ok(AsyncConnectionWrapper::from(connection))
}
*/

pub async fn execute_with_better_error<U>(
    conn: &mut MyDbConnection,
    query: U,
    mut additional_where_clause: Option<&'static str>,
) -> QueryResult<usize>
where
    U: QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
{
    let original_query = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
    // This is needed because if we don't insert any row, then diesel makes a call like this
    // SELECT 1 FROM TABLE WHERE 1=0
    if original_query.to_lowercase().contains("where") {
        additional_where_clause = None;
    }
    let final_query = UpsertFilterLatestTransactionQuery {
        query,
        where_clause: additional_where_clause,
    };
    let debug_string = diesel::debug_query::<diesel::pg::Pg, _>(&final_query).to_string();
    tracing::debug!("Executing query: {:?}", debug_string);
    let res = final_query.execute(conn).await;
    if let Err(ref e) = res {
        tracing::warn!("Error running query: {:?}\n{:?}", e, debug_string);
    }
    res
}

pub async fn run_pending_migrations(conn: &mut MyDbConnection) {
    MIGRATIONS
        .run_pending_migrations(conn)
        .await
        .expect("[Parser] Migrations failed!");
}

/// Section below is required to modify the query.
impl<T: Query> Query for UpsertFilterLatestTransactionQuery<T> {
    type SqlType = T::SqlType;
}

//impl<T> RunQueryDsl<MyDbConnection> for UpsertFilterLatestTransactionQuery<T> {}

impl<T> QueryFragment<Pg> for UpsertFilterLatestTransactionQuery<T>
where
    T: QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        self.query.walk_ast(out.reborrow())?;
        if let Some(w) = self.where_clause {
            out.push_sql(w);
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_get_chunks_logic() {
        assert_eq!(get_chunks(10, 5), vec![(0, 10)]);
        assert_eq!(get_chunks(65535, 1), vec![
            (0, 32767),
            (32767, 65534),
            (65534, 65535)
        ]);
        // 200,000 total items will take 6 buckets. Each bucket can only be 3276 size.
        assert_eq!(get_chunks(10000, 20), vec![
            (0, 1638),
            (1638, 3276),
            (3276, 4914),
            (4914, 6552),
            (6552, 8190),
            (8190, 9828),
            (9828, 10000)
        ]);
        assert_eq!(get_chunks(65535, 2), vec![
            (0, 16383),
            (16383, 32766),
            (32766, 49149),
            (49149, 65532),
            (65532, 65535)
        ]);
    }
}
