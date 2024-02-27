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
use diesel_async::RunQueryDsl;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;
use tracing::info;

// pub type fn(&'a [Item]) -> (Query, Option<&'static str>) = fn(&'a [Item]) -> (Query, Option<&'static str>);

/// A simple holder that allows us to move query generators cross threads and avoid cloning
pub struct QueryGenerator<'a, Query, Item>
    where
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    pub build_query_fn: fn(&'a [Item]) -> (Query, Option<&'static str>),
    pub items: Vec<Item>,
    pub table_name: &'static str,
}

impl<'a, Query, Item> QueryGenerator<'a, Query, Item>
    where
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    pub fn new(
        table_name: &'static str,
        items: Vec<Item>,
        build_query_fn: fn(&'a [Item]) -> (Query, Option<&'static str>),
    ) -> Self {
        Self {
            table_name,
            build_query_fn,
            items,
        }
    }

    /*
    pub fn cleaned_query(
        self,
    ) -> (
        Box<dyn QueryFragment<diesel::pg::Pg> + Send + Sync>,
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
pub trait DbExecutable: Send + Sync {
    async fn execute_query(self: Box<Self>, conn: PgDbPool) -> QueryResult<usize>;
}

#[async_trait::async_trait]
impl<'a, Query, Item> DbExecutable for QueryGenerator<'a, Query, Item>
    where
        Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    async fn execute_query(self: Box<Self>, conn: PgDbPool) -> QueryResult<usize> {
        let Self {
            build_query_fn,
            items,
            ..
        } = *self;

        //let items_ref: &'a [Item] = &items;
        let items_ref = unsafe { std::mem::transmute::<&[Item], &'a [Item]>(&items) };
        let (query, _) = (build_query_fn)(items_ref);
        drop(query);
        drop(items);
        Ok(0)
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
        let query_receiver = query_receiver.clone();
        loop {
            match query_receiver.recv().await {
                Ok(query_generator) => {
                    query_generator
                        .execute_query(conn.clone())
                        .await
                        .expect("Error executing query");
                }
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
pub async fn execute_in_chunks<'a, Query, Item>(
    table_name: &'static str,
    query_sender: AsyncSender<Box<dyn DbExecutable>>,
    build_query: fn(&'a [Item]) -> (Query, Option<&'static str>),
    items_to_insert: Vec<Item>,
    chunk_size: usize,
) where
    Query:
    QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a + 'static,
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
