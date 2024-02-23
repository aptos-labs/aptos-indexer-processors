/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{
    utils::database::{clean_data_for_db, get_chunks, PgDbPool},
    worker::PROCESSOR_SERVICE_TYPE,
};
use diesel::{query_builder::QueryFragment, QueryResult};
use diesel_async::{methods::ExecuteDsl, RunQueryDsl};
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;
use tracing::info;

pub type QueryGeneratorFn<
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
> = fn(&[Item]) -> (Box<Query>, Option<&'static str>);

/// A simple holder that allows us to move query generators cross threads and avoid cloning
pub struct QueryGenerator<Query, Item>
    where
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'static,
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    pub build_query_fn: QueryGeneratorFn<Query, Item>,
    pub items: Vec<Item>,
    pub table_name: &'static str,
}

impl<Query, Item> QueryGenerator<Query, Item>
    where
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'static,
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    pub fn new(
        table_name: &'static str,
        items: Vec<Item>,
        build_query_fn: QueryGeneratorFn<Query, Item>,
    ) -> Self {
        Self {
            table_name,
            build_query_fn,
            items,
        }
    }

    pub fn generate_query(&self) -> (Box<Query>, Option<&'static str>) {
        (self.build_query_fn)(&self.items)
    }

    pub fn num_items(&self) -> usize {
        self.items.len()
    }

    /*
    pub fn cleaned_query(
        self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + Sync + 'static>,
        Option<&'static str>,
    ) {
        let QueryGenerator {
            items,
            build_query_fn,
        } = self;
        let cleaned_items = clean_data_for_db(items, true);
        Self::new(cleaned_items, build_query_fn).generate_query()
    }
     */
}

#[async_trait::async_trait]
pub trait DbExecutable {
    async fn execute_query(&self, conn: PgDbPool) -> QueryResult<usize>;
}

#[async_trait::async_trait]
impl<Query, Item> DbExecutable for QueryGenerator<Query, Item>
    where
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'static,
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    async fn execute_query(&self, conn: PgDbPool) -> QueryResult<usize> {
        let (query, _) = self.generate_query();
        // TODO: retry
        // TODO: handle the cleaning!!!
        execute_with_better_error(conn.clone(), query).await
    }
}

// A holder struct for processors db writing so we don't need to keep adding new params
#[derive(Clone)]
pub struct DbWriter {
    pub db_pool: PgDbPool,
    pub query_sender: AsyncSender<Box<dyn DbExecutable>>,
}

impl DbWriter {
    pub fn new(db_pool: PgDbPool, query_sender: AsyncSender<Box<dyn DbExecutable>>) -> Self {
        Self {
            db_pool,
            query_sender,
        }
    }
}

pub async fn launch_db_writer_tasks(
    query_receiver: AsyncReceiver<Box<dyn DbExecutable>>,
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

pub fn launch_db_writer_task(
    query_receiver: AsyncReceiver<Box<dyn DbExecutable>>,
    processor_name: &'static str,
    conn: PgDbPool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match query_receiver.recv().await {
                Ok(query_generator) => execute_or_retry(conn.clone(), query_generator)
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

/// Sends the query to the db writer channel
pub async fn execute_in_chunks<Query, Item>(
    table_name: &'static str,
    query_sender: AsyncSender<Box<dyn DbExecutable>>,
    build_query: QueryGeneratorFn<Query, Item>,
    items_to_insert: Vec<Item>,
    chunk_size: usize,
) where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    let chunks = get_chunks(items_to_insert.len(), chunk_size);
    for (start_ind, end_ind) in chunks {
        let items = items_to_insert[start_ind..end_ind].to_vec();
        let query_generator = QueryGenerator::new(table_name, items, build_query);
        query_sender
            .send(Box::new(query_generator))
            .await
            .expect("Error sending query generator to db writer task");
    }
}

async fn execute_or_retry(
    conn: PgDbPool,
    db_executable: Box<dyn DbExecutable>,
) -> Result<(), diesel::result::Error> {
    // TODO: retry
    // TODO: handle the cleaning!!!
    match db_executable.execute_query(conn).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn execute_with_better_error<Query>(pool: PgDbPool, query: Query) -> QueryResult<usize>
    where
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync,
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
