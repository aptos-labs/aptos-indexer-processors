/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{
    data_with_query::DataWithQuery,
    utils::{
        counters::{
            DB_EXECUTION_RETRIES_COUNT, DB_EXECUTOR_CHANNEL_CHUNK_INSERT_COUNT,
            DB_EXECUTOR_CHANNEL_SIZE, DB_INSERTION_ROWS_COUNT, DB_NULL_BYTE_INSERT_COUNT,
            DB_SINGLE_BATCH_EXECUTION_TIME_IN_SECS,
        },
        database::{get_chunks, get_config_table_chunk_size, PgDbPool},
        util::remove_null_bytes,
    },
    worker::PROCESSOR_SERVICE_TYPE,
};
use ahash::{AHashMap, AHashSet};
use diesel::{
    query_builder::QueryFragment,
    result::{DatabaseErrorKind, Error},
    QueryResult,
};
use kanal::{AsyncReceiver, AsyncSender};
use std::{borrow::Cow, future::Future, pin::Pin};
use tokio::task::JoinHandle;
use tracing::info;

pub trait DbCleanable {
    fn clean(&mut self);
}

impl<Item> DbCleanable for Vec<Item>
where
    Item: field_count::FieldCount
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Clone,
{
    fn clean(&mut self) {
        *self = self.iter().map(remove_null_bytes).collect()
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait DbRowCountable {
    fn len(&self) -> usize;
}

impl<Item> DbRowCountable for Vec<Item>
where
    Item: Send,
{
    fn len(&self) -> usize {
        self.len()
    }
}

#[async_trait::async_trait]
/// The `DbExecutable` trait is for types that can be executed in the database
pub trait DbExecutable: DbCleanable + DbRowCountable + Send {
    async fn execute_query(&self, conn: PgDbPool) -> QueryResult<usize>;

    // TODO: make this config/constant?
    fn max_retries(&self) -> usize {
        2
    }

    async fn execute_or_retry_cleaned(
        &mut self,
        processor_name: &str,
        table_name: &str,
        conn: PgDbPool,
    ) -> QueryResult<usize> {
        let insertion_timer = std::time::Instant::now();

        let mut res = self.execute_query(conn.clone()).await;

        // TODO: HAVE BETTER RETRY LOGIC HERE?
        let mut num_tries = 0;
        let max_retries = self.max_retries();
        while num_tries < max_retries {
            match &res {
                // If res is a success, we can terminate this loop
                Ok(_) => break,
                Err(e) => {
                    num_tries += 1;
                    let error_type = diesel_error_to_metric_str(e);
                    DB_EXECUTION_RETRIES_COUNT
                        .with_label_values(&[processor_name, table_name, error_type])
                        .inc();

                    // TODO: handle CDB retries differently here (higher retry count?)

                    if let Error::DatabaseError(DatabaseErrorKind::Unknown, db_err) = e {
                        // If this is a PG null byte error, clean and retry, otherwise retry and/or return the error
                        if db_err
                            .message()
                            .contains("invalid byte sequence for encoding \"UTF8\"")
                        {
                            DB_NULL_BYTE_INSERT_COUNT
                                .with_label_values(&[processor_name, table_name])
                                .inc();
                            self.clean();
                            res = self.execute_query(conn.clone()).await;
                            // Go to next loop iteration to see if this was OK or Err
                            continue;
                        }
                    }

                    // Sleep a bit so we're not immediately coming back to this task
                    // TODO: make this configurable?
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    // Then we retry
                    res = self.execute_query(conn.clone()).await;
                },
            }
        }

        DB_SINGLE_BATCH_EXECUTION_TIME_IN_SECS
            .with_label_values(&[processor_name, table_name])
            .set(insertion_timer.elapsed().as_secs_f64());

        DB_INSERTION_ROWS_COUNT
            .with_label_values(&[processor_name, table_name])
            .inc_by(self.len() as u64);
        res
    }
}

pub fn diesel_error_to_metric_str(error: &Error) -> &'static str {
    match error {
        Error::DatabaseError(kind, _) => match kind {
            DatabaseErrorKind::UniqueViolation => "DatabaseError::UniqueViolation",
            DatabaseErrorKind::ForeignKeyViolation => "DatabaseError::ForeignKeyViolation",
            DatabaseErrorKind::UnableToSendCommand => "DatabaseError::UnableToSendCommand",
            DatabaseErrorKind::SerializationFailure => "DatabaseError::SerializationFailure",
            DatabaseErrorKind::ReadOnlyTransaction => "DatabaseError::ReadOnlyTransaction",
            DatabaseErrorKind::NotNullViolation => "DatabaseError::NotNullViolation",
            DatabaseErrorKind::CheckViolation => "DatabaseError::CheckViolation",
            DatabaseErrorKind::ClosedConnection => "DatabaseError::ClosedConnection",
            DatabaseErrorKind::Unknown => "DatabaseError::Unknown",
            _ => "DatabaseError::Other",
        },
        Error::NotFound => "NotFound",
        Error::QueryBuilderError(_) => "QueryBuilderError",
        Error::DeserializationError(_) => "DeserializationError",
        Error::SerializationError(_) => "SerializationError",
        Error::InvalidCString(_) => "InvalidCString",
        Error::RollbackTransaction => "RollbackTransaction",
        Error::AlreadyInTransaction => "AlreadyInTransaction",
        Error::RollbackErrorOnCommit { .. } => "RollbackErrorOnCommit",
        Error::NotInTransaction => "NotInTransaction",
        Error::BrokenTransactionManager => "BrokenTransactionManager",
        _ => "Other",
    }
}

// A holder struct for processors db writing so we don't need to keep adding new params
#[derive(Clone)]
pub struct DbWriter {
    pub processor_name: &'static str,
    pub db_pool: PgDbPool,
    pub query_sender: AsyncSender<QueryGenerator>,
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    pub skip_tables: AHashSet<String>,
}

impl DbWriter {
    pub fn new(
        processor_name: &'static str,
        db_pool: PgDbPool,
        query_sender: AsyncSender<QueryGenerator>,
        per_table_chunk_sizes: AHashMap<String, usize>,
        skip_tables: AHashSet<String>,
    ) -> Self {
        Self {
            processor_name,
            db_pool,
            query_sender,
            per_table_chunk_sizes,
            skip_tables,
        }
    }

    pub fn chunk_size<Item: field_count::FieldCount>(&self, table_name: &str) -> usize {
        get_config_table_chunk_size::<Item>(table_name, &self.per_table_chunk_sizes)
    }

    pub async fn send_in_chunks_with_query<'a, Item, Query>(
        &self,
        table_name: &'static str,
        items_to_insert: Vec<Item>,
        query_fn: fn(&'a [Item]) -> Query,
    ) where
        Item: field_count::FieldCount
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + Send
            + Sync
            + Clone
            + 'static,
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    {
        // TODO: ideally this kind of skip is done much earlier in the process
        if self.skip_tables.contains(table_name) {
            return;
        }

        DB_EXECUTOR_CHANNEL_CHUNK_INSERT_COUNT
            .with_label_values(&[self.processor_name, table_name])
            .inc();

        let chunk_size = self.chunk_size::<Item>(table_name);
        let chunks = get_chunks(items_to_insert.len(), chunk_size);
        for (start_ind, end_ind) in chunks {
            let items = items_to_insert[start_ind..end_ind].to_vec();
            // COW the items
            let items = items.into();
            let data_with_query = DataWithQuery::new(items, query_fn);

            self.query_sender
                .send(QueryGenerator {
                    table_name,
                    db_executable: Box::new(data_with_query),
                })
                .await
                .expect("Error sending query generator to db writer task");
        }

        DB_EXECUTOR_CHANNEL_SIZE
            .with_label_values(&[self.processor_name])
            .set(self.query_sender.len() as i64);
    }

    pub async fn send_in_chunks<Item>(&self, table_name: &'static str, items_to_insert: Vec<Item>)
    where
        Item: field_count::FieldCount
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + Send
            + Clone
            + 'static,
        Vec<Item>: DbExecutable,
    {
        // TODO: ideally this kind of skip is done much earlier in the process
        if self.skip_tables.contains(table_name) {
            return;
        }

        DB_EXECUTOR_CHANNEL_CHUNK_INSERT_COUNT
            .with_label_values(&[self.processor_name, table_name])
            .inc();

        let chunk_size = self.chunk_size::<Item>(table_name);
        let chunks = get_chunks(items_to_insert.len(), chunk_size);
        for (start_ind, end_ind) in chunks {
            let items = items_to_insert[start_ind..end_ind].to_vec();
            self.query_sender
                .send(QueryGenerator {
                    table_name,
                    db_executable: Box::new(items),
                })
                .await
                .expect("Error sending query generator to db writer task");
        }

        DB_EXECUTOR_CHANNEL_SIZE
            .with_label_values(&[self.processor_name])
            .set(self.query_sender.len() as i64);
    }
}

pub async fn launch_db_writer_tasks(
    query_receiver: AsyncReceiver<QueryGenerator>,
    processor_name: &'static str,
    num_tasks: usize,
    conn: PgDbPool,
) {
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        "[Parser] Starting db writer tasks",
    );
    let tasks = (0..num_tasks)
        .map(|_| launch_db_writer_task(query_receiver.clone(), processor_name, conn.clone()))
        .collect::<Vec<_>>();
    futures::future::try_join_all(tasks)
        .await
        .expect("Error joining db writer tasks");
}

// This is a wrapper struct that allows us to pass additional data alongside the DbExecutable
pub struct QueryGenerator {
    pub table_name: &'static str,
    pub db_executable: Box<dyn DbExecutable>,
}

pub fn launch_db_writer_task(
    query_receiver: AsyncReceiver<QueryGenerator>,
    processor_name: &'static str,
    conn: PgDbPool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match query_receiver.recv().await {
                Ok(mut query_generator) => {
                    let query_res = query_generator
                        .db_executable
                        .execute_or_retry_cleaned(
                            processor_name,
                            query_generator.table_name,
                            conn.clone(),
                        )
                        .await;
                    query_res.expect("Error executing query");
                    drop(query_generator);
                },
                Err(e) => info!(
                    processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    error = ?e,
                    "[Parser] DB writer task channel has been closed",
                ),
            };
        }
    })
}

pub async fn execute_with_better_error<Query>(pool: PgDbPool, query: Query) -> QueryResult<usize>
where
    Query: diesel_async::RunQueryDsl<crate::utils::database::MyDbConnection>
        + diesel::query_builder::QueryFragment<diesel::pg::Pg>
        + diesel::query_builder::QueryId
        + Send
        + Sync,
{
    let debug_string = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
    tracing::debug!("Executing query: {:?}", debug_string);
    let conn = &mut pool.get().await.map_err(|e| {
        tracing::warn!("Error getting connection from pool: {:?}", e);
        diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::Unknown,
            Box::new(e.to_string()),
        )
    })?;
    let res = query.execute(conn).await;
    // TODO: this is pretty expensive
    if let Err(ref e) = res {
        tracing::warn!("Error running query: {:?}\n{:?}", e, debug_string);
    }
    res
}
