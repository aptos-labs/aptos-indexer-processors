/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{
    utils::database::{get_chunks, PgDbPool},
    worker::PROCESSOR_SERVICE_TYPE,
};
use diesel::{pg::Pg, query_builder::QueryFragment, QueryResult};
use diesel_async::RunQueryDsl;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;
use tracing::info;

pub struct BoxedQuery<'a> {
    query: Box<dyn QueryFragment<Pg> + Send + 'a>,
}

impl<'a> BoxedQuery<'a> {
    fn new(query: impl QueryFragment<Pg> + Send + 'a) -> Self {
        Self {
            query: Box::new(query),
        }
    }
}

pub trait BoxedQueryTrait<'a>: 'a {
    fn boxed_query(self) -> BoxedQuery<'a>;
}

impl<'a, Query> BoxedQueryTrait<'a> for Query
    where
        Query: QueryFragment<Pg> + Send + 'a,
{
    fn boxed_query(self) -> BoxedQuery<'a> {
        BoxedQuery::new(self)
    }
}

// pub type fn(&'a [Item]) -> (Query, Option<&'static str>) = fn(&'a [Item]) -> (Query, Option<&'static str>);

/// A simple holder that allows us to move query generators cross threads and avoid cloning
pub struct QueryGenerator<Item> {
    pub build_query_fn: fn(&[Item]) -> (BoxedQuery, Option<&'static str>),
    pub items: Vec<Item>,
    pub table_name: &'static str,
}

impl<Item> QueryGenerator<Item>
    where
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    pub fn new(
        table_name: &'static str,
        items: Vec<Item>,
        build_query_fn: fn(&[Item]) -> (BoxedQuery, Option<&'static str>),
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
impl<Item> DbExecutable for QueryGenerator<Item>
    where
        Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    async fn execute_query(self: Box<Self>, pool: PgDbPool) -> QueryResult<usize> {
        //let items_ref: &'a [Item] = &items;
        let (query, _) = (self.build_query_fn)(&self.items);

        let conn = &mut pool
            .get()
            .await
            .map_err(|e| {
                tracing::warn!("Error getting connection from pool: {:?}", e);
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UnableToSendCommand,
                    Box::new(e.to_string()),
                )
            })
            .expect("Error getting connection from pool");

        query
            .query
            .execute(conn)
            .await
            .expect("Error executing query");

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
pub async fn execute_in_chunks<Item>(
    table_name: &'static str,
    query_sender: AsyncSender<Box<dyn DbExecutable>>,
    build_query: fn(&[Item]) -> (BoxedQuery, Option<&'static str>),
    items_to_insert: Vec<Item>,
    chunk_size: usize,
) where
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
