/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{
    utils::database::{clean_data_for_db, get_chunks, PgDbPool},
    worker::PROCESSOR_SERVICE_TYPE,
};
use diesel::{pg::Pg, query_builder::QueryFragment, QueryResult};
use diesel_async::RunQueryDsl;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;
use tracing::info;

/*
pub type QueryGeneratorFn<
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send ,
> = fn(&[Item]) -> (Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static >, Option<&'static str>);
*/

pub trait GeneratesQuery<Item>
where
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
{
    /// Generates the query. This does not clone!
    fn generate_query(
        &self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    );

    /// Gets the length of the inner item vec
    fn num_items(&self) -> usize;

    /// To avoid a clone of items, this consumes self, cleans the items- and returns the cleaned query
    fn cleaned_query(
        self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    );
}

// A new, object-safe wrapper trait
pub trait AnyGeneratesQuery {
    /// Generates the query. This does not clone!
    fn generate_query(
        &self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    );

    /// Gets the length of the inner item vec
    fn num_items(&self) -> usize;

    /// To avoid a clone of items, this consumes self, cleans the items- and returns the cleaned query
    fn cleaned_query(
        self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    );
}

impl<Item> AnyGeneratesQuery for dyn GeneratesQuery<Item>
where
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
{
    fn generate_query(
        &self,
    ) -> (
        Box<dyn QueryFragment<Pg> + Send + 'static>,
        Option<&'static str>,
    ) {
        <dyn self::GeneratesQuery<Item>>::generate_query(self)
    }

    fn num_items(&self) -> usize {
        <dyn self::GeneratesQuery<Item>>::num_items(self)
    }

    fn cleaned_query(
        self,
    ) -> (
        Box<dyn QueryFragment<Pg> + Send + 'static>,
        Option<&'static str>,
    ) {
        <dyn self::GeneratesQuery<Item>>::cleaned_query(self)
    }
}

/// A simple holder that allows us to move query generators cross threads and avoid cloning
pub struct QueryGenerator<Item>
where
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
{
    build_query_fn: fn(
        &[Item],
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    ),
    items: Vec<Item>,
}

impl<Item> QueryGenerator<Item>
where
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
{
    pub fn new(
        items: Vec<Item>,
        build_query_fn: fn(
            &[Item],
        ) -> (
            Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
            Option<&'static str>,
        ),
    ) -> Self {
        Self {
            build_query_fn,
            items,
        }
    }
}

impl<Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static>
    GeneratesQuery<Item> for QueryGenerator<Item>
{
    fn generate_query(
        &self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    ) {
        (self.build_query_fn)(&self.items)
    }

    fn num_items(&self) -> usize {
        self.items.len()
    }

    fn cleaned_query(
        self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    ) {
        let QueryGenerator {
            items,
            build_query_fn,
        } = self;
        let cleaned_items = clean_data_for_db(items, true);
        Self::new(cleaned_items, build_query_fn).generate_query()
    }
}

// A holder struct for processors db writing so we don't need to keep adding new params
#[derive(Clone)]
pub struct DbWriter {
    pub db_pool: PgDbPool,
    pub query_sender: AsyncSender<Box<dyn AnyGeneratesQuery>>,
}

impl DbWriter {
    pub fn new(db_pool: PgDbPool, query_sender: AsyncSender<Box<dyn AnyGeneratesQuery>>) -> Self {
        Self {
            db_pool,
            query_sender,
        }
    }
}

pub async fn launch_db_writer_tasks(
    query_receiver: AsyncReceiver<Box<dyn AnyGeneratesQuery>>,
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
    query_receiver: AsyncReceiver<Box<dyn AnyGeneratesQuery>>,
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
pub async fn execute_in_chunks<Item>(
    query_sender: AsyncSender<Box<dyn AnyGeneratesQuery>>,
    build_query: fn(
        &[Item],
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
        Option<&'static str>,
    ),
    items_to_insert: Vec<Item>,
    chunk_size: usize,
) where
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send,
{
    let chunks = get_chunks(items_to_insert.len(), chunk_size);
    for (start_ind, end_ind) in chunks {
        let items = items_to_insert[start_ind..end_ind].to_vec();
        let query_generator = QueryGenerator::new(items, build_query);
        query_sender
            .send(Box::new(query_generator))
            .await
            .expect("Error sending query generator to db writer task");
    }
}

async fn execute_or_retry(
    conn: PgDbPool,
    query_generator: Box<dyn AnyGeneratesQuery>,
) -> Result<(), diesel::result::Error> {
    // TODO: retry
    // TODO: handle the cleaning!!!
    let (query, _) = query_generator.generate_query();
    match execute_with_better_error(conn.clone(), query).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn execute_with_better_error(
    pool: PgDbPool,
    query: Box<dyn QueryFragment<diesel::pg::Pg> + Send + 'static>,
) -> QueryResult<usize> {
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
