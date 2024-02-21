/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{utils::database::PgDbPool, worker::PROCESSOR_SERVICE_TYPE};
use diesel::QueryResult;
use diesel_async::RunQueryDsl;
use kanal::AsyncReceiver;
use tokio::task::JoinHandle;
use tracing::info;

pub async fn launch_db_writer_tasks<
    Query: diesel::query_builder::QueryFragment<diesel::pg::Pg>
    + diesel::query_builder::QueryId
    + Send
    + 'static,
>(
    query_receiver: AsyncReceiver<Query>,
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

pub fn launch_db_writer_task<
    Query: diesel::query_builder::QueryFragment<diesel::pg::Pg>
    + diesel::query_builder::QueryId
    + Send
    + 'static,
>(
    query_receiver: AsyncReceiver<Query>,
    processor_name: &'static str,
    conn: PgDbPool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match query_receiver.recv().await {
                Ok(query) => execute_or_retry(conn.clone(), query)
                    .await
                    .expect("Error executing query"),
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

async fn execute_or_retry<Query>(conn: PgDbPool, query: Query) -> Result<(), diesel::result::Error>
    where
        Query: diesel::query_builder::QueryFragment<diesel::pg::Pg>
        + diesel::query_builder::QueryId
        + Send,
{
    // TODO: retry
    match execute_with_better_error(conn.clone(), query).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn execute_with_better_error<Query>(pool: PgDbPool, query: Query) -> QueryResult<usize>
    where
        Query: diesel::query_builder::QueryFragment<diesel::pg::Pg>
        + diesel::query_builder::QueryId
        + Send,
{
    let debug_string = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
    tracing::debug!("Executing query: {:?}", debug_string);
    let conn = &mut pool.get().await.map_err(|e| {
        tracing::warn!("Error getting connection from pool: {:?}", e);
        diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::UnableToSendCommand,
            Box::new(e.to_string()),
        )
    })?;
    let res = query.execute(conn).await;
    if let Err(ref e) = res {
        tracing::warn!("Error running query: {:?}\n{:?}", e, debug_string);
    }
    res
}
