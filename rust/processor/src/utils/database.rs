// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Database-related functions
#![allow(clippy::extra_unused_lifetimes)]

use crate::utils::util::remove_null_bytes;
use ahash::AHashMap;
use diesel::{
    query_builder::{AstPass, Query, QueryFragment},
    ConnectionResult, QueryResult,
};
use diesel_async::{
    pooled_connection::{
        bb8::{Pool, PooledConnection},
        AsyncDieselConnectionManager, ManagerConfig, PoolError,
    },
    AsyncPgConnection, RunQueryDsl,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures_util::{future::BoxFuture, FutureExt};
use std::{cmp::min, sync::Arc};

pub type Backend = diesel::pg::Pg;

pub type MyDbConnection = AsyncPgConnection;
pub type DbPool = Pool<MyDbConnection>;
pub type ArcDbPool = Arc<DbPool>;
pub type DbPoolConnection<'a> = PooledConnection<'a, MyDbConnection>;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("src/db/postgres/migrations");

pub const DEFAULT_MAX_POOL_SIZE: u32 = 150;

#[derive(QueryId)]
/// Using this will append a where clause at the end of the string upsert function, e.g.
/// INSERT INTO ... ON CONFLICT DO UPDATE SET ... WHERE "transaction_version" = excluded."transaction_version"
/// This is needed when we want to maintain a table with only the latest state
pub struct UpsertFilterLatestTransactionQuery<T> {
    query: T,
    where_clause: Option<&'static str>,
}

// the max is actually u16::MAX but we see that when the size is too big we get an overflow error so reducing it a bit
pub const MAX_DIESEL_PARAM_SIZE: usize = (u16::MAX / 2) as usize;

/// This function returns boundaries of chunks in the form of (start_index, end_index)
pub fn get_chunks(num_items_to_insert: usize, chunk_size: usize) -> Vec<(usize, usize)> {
    let mut chunk: (usize, usize) = (0, min(num_items_to_insert, chunk_size));
    let mut chunks = vec![chunk];
    while chunk.1 != num_items_to_insert {
        chunk = (
            chunk.0 + chunk_size,
            min(num_items_to_insert, chunk.1 + chunk_size),
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

fn establish_connection(database_url: &str) -> BoxFuture<ConnectionResult<AsyncPgConnection>> {
    use native_tls::{Certificate, TlsConnector};
    use postgres_native_tls::MakeTlsConnector;

    (async move {
        let (url, cert_path) = parse_and_clean_db_url(database_url);
        let cert = std::fs::read(cert_path.unwrap()).expect("Could not read certificate");

        let cert = Certificate::from_pem(&cert).expect("Could not parse certificate");
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .add_root_certificate(cert)
            .build()
            .expect("Could not build TLS connector");
        let connector = MakeTlsConnector::new(connector);

        let (client, connection) = tokio_postgres::connect(&url, connector)
            .await
            .expect("Could not connect to database");
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        AsyncPgConnection::try_from(client).await
    })
    .boxed()
}

fn parse_and_clean_db_url(url: &str) -> (String, Option<String>) {
    let mut db_url = url::Url::parse(url).expect("Could not parse database url");
    let mut cert_path = None;

    let mut query = "".to_string();
    db_url.query_pairs().for_each(|(k, v)| {
        if k == "sslrootcert" {
            cert_path = Some(v.parse().unwrap());
        } else {
            query.push_str(&format!("{}={}&", k, v));
        }
    });
    db_url.set_query(Some(&query));

    (db_url.to_string(), cert_path)
}

pub async fn new_db_pool(
    database_url: &str,
    max_pool_size: Option<u32>,
) -> Result<ArcDbPool, PoolError> {
    let (_url, cert_path) = parse_and_clean_db_url(database_url);

    let config = if cert_path.is_some() {
        let mut config = ManagerConfig::<MyDbConnection>::default();
        config.custom_setup = Box::new(|conn| Box::pin(establish_connection(conn)));
        AsyncDieselConnectionManager::<MyDbConnection>::new_with_config(database_url, config)
    } else {
        AsyncDieselConnectionManager::<MyDbConnection>::new(database_url)
    };
    let pool = Pool::builder()
        .max_size(max_pool_size.unwrap_or(DEFAULT_MAX_POOL_SIZE))
        .build(config)
        .await?;
    Ok(Arc::new(pool))
}

pub async fn execute_in_chunks<U, T>(
    conn: ArcDbPool,
    build_query: fn(Vec<T>) -> (U, Option<&'static str>),
    items_to_insert: &[T],
    chunk_size: usize,
) -> Result<(), diesel::result::Error>
where
    U: QueryFragment<Backend> + diesel::query_builder::QueryId + Send + 'static,
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send + 'static,
{
    let chunks = get_chunks(items_to_insert.len(), chunk_size);

    let tasks = chunks
        .into_iter()
        .map(|(start_ind, end_ind)| {
            let items = items_to_insert[start_ind..end_ind].to_vec();
            let conn = conn.clone();
            tokio::spawn(async move {
                let (query, additional_where_clause) = build_query(items.clone());
                execute_or_retry_cleaned(conn, build_query, items, query, additional_where_clause)
                    .await
            })
        })
        .collect::<Vec<_>>();

    let results = futures_util::future::try_join_all(tasks)
        .await
        .expect("Task panicked executing in chunks");
    for res in results {
        res?
    }

    Ok(())
}

pub async fn execute_with_better_error<U>(
    pool: ArcDbPool,
    query: U,
    mut additional_where_clause: Option<&'static str>,
) -> QueryResult<usize>
where
    U: QueryFragment<Backend> + diesel::query_builder::QueryId + Send,
{
    let original_query = diesel::debug_query::<Backend, _>(&query).to_string();
    // This is needed because if we don't insert any row, then diesel makes a call like this
    // SELECT 1 FROM TABLE WHERE 1=0
    if original_query.to_lowercase().contains("where") {
        additional_where_clause = None;
    }
    let final_query = UpsertFilterLatestTransactionQuery {
        query,
        where_clause: additional_where_clause,
    };
    let debug_string = diesel::debug_query::<Backend, _>(&final_query).to_string();
    tracing::debug!("Executing query: {:?}", debug_string);
    let conn = &mut pool.get().await.map_err(|e| {
        tracing::warn!("Error getting connection from pool: {:?}", e);
        diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::UnableToSendCommand,
            Box::new(e.to_string()),
        )
    })?;
    let res = final_query.execute(conn).await;
    if let Err(ref e) = res {
        tracing::warn!("Error running query: {:?}\n{:?}", e, debug_string);
    }
    res
}

/// Returns the entry for the config hashmap, or the default field count for the insert
/// Given diesel has a limit of how many parameters can be inserted in a single operation (u16::MAX),
/// we default to chunk an array of items based on how many columns are in the table.
pub fn get_config_table_chunk_size<T: field_count::FieldCount>(
    table_name: &str,
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> usize {
    per_table_chunk_sizes
        .get(table_name)
        .copied()
        .unwrap_or_else(|| MAX_DIESEL_PARAM_SIZE / T::field_count())
}

pub async fn execute_with_better_error_conn<U>(
    conn: &mut MyDbConnection,
    query: U,
    mut additional_where_clause: Option<&'static str>,
) -> QueryResult<usize>
where
    U: QueryFragment<Backend> + diesel::query_builder::QueryId + Send,
{
    let original_query = diesel::debug_query::<Backend, _>(&query).to_string();
    // This is needed because if we don't insert any row, then diesel makes a call like this
    // SELECT 1 FROM TABLE WHERE 1=0
    if original_query.to_lowercase().contains("where") {
        additional_where_clause = None;
    }
    let final_query = UpsertFilterLatestTransactionQuery {
        query,
        where_clause: additional_where_clause,
    };
    let debug_string = diesel::debug_query::<Backend, _>(&final_query).to_string();
    tracing::debug!("Executing query: {:?}", debug_string);
    let res = final_query.execute(conn).await;
    if let Err(ref e) = res {
        tracing::warn!("Error running query: {:?}\n{:?}", e, debug_string);
    }
    res
}

async fn execute_or_retry_cleaned<U, T>(
    conn: ArcDbPool,
    build_query: fn(Vec<T>) -> (U, Option<&'static str>),
    items: Vec<T>,
    query: U,
    additional_where_clause: Option<&'static str>,
) -> Result<(), diesel::result::Error>
where
    U: QueryFragment<Backend> + diesel::query_builder::QueryId + Send,
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone,
{
    match execute_with_better_error(conn.clone(), query, additional_where_clause).await {
        Ok(_) => {},
        Err(_) => {
            let cleaned_items = clean_data_for_db(items, true);
            let (cleaned_query, additional_where_clause) = build_query(cleaned_items);
            match execute_with_better_error(conn.clone(), cleaned_query, additional_where_clause)
                .await
            {
                Ok(_) => {},
                Err(e) => {
                    return Err(e);
                },
            }
        },
    }
    Ok(())
}

pub fn run_pending_migrations<DB: diesel::backend::Backend>(conn: &mut impl MigrationHarness<DB>) {
    conn.run_pending_migrations(MIGRATIONS)
        .expect("[Parser] Migrations failed!");
}

/// Section below is required to modify the query.
impl<T: Query> Query for UpsertFilterLatestTransactionQuery<T> {
    type SqlType = T::SqlType;
}

//impl<T> RunQueryDsl<MyDbConnection> for UpsertFilterLatestTransactionQuery<T> {}

impl<T> QueryFragment<Backend> for UpsertFilterLatestTransactionQuery<T>
where
    T: QueryFragment<Backend>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Backend>) -> QueryResult<()> {
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
            (65534, 65535),
        ]);
        // 200,000 total items will take 6 buckets. Each bucket can only be 3276 size.
        assert_eq!(get_chunks(10000, 20), vec![
            (0, 1638),
            (1638, 3276),
            (3276, 4914),
            (4914, 6552),
            (6552, 8190),
            (8190, 9828),
            (9828, 10000),
        ]);
        assert_eq!(get_chunks(65535, 2), vec![
            (0, 16383),
            (16383, 32766),
            (32766, 49149),
            (49149, 65532),
            (65532, 65535),
        ]);
    }
}
