/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{
    db_writer::{DbCleanable, DbExecutable, DbRowCountable},
    utils::{database::PgDbPool, util::remove_null_bytes},
};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, future::Future, pin::Pin};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestModel {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestDb {}

pub struct DataWithQuery<Item, F>
where
    Item: field_count::FieldCount
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Clone,
    F: Fn(
            Cow<'static, [Item]>,
            PgDbPool,
        ) -> Pin<Box<dyn Future<Output = QueryResult<usize>> + Send + 'static>>
        + Send
        + Sync
        + Copy
        + 'static,
{
    data: Vec<Item>,
    query_fn: F,
}

impl<Item, F> DataWithQuery<Item, F>
where
    Item: field_count::FieldCount
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Clone,
    F: Fn(
            Cow<'static, [Item]>,
            PgDbPool,
        ) -> Pin<Box<dyn Future<Output = QueryResult<usize>> + Send + 'static>>
        + Send
        + Sync
        + Copy
        + 'static,
{
    pub fn new(data: Vec<Item>, query_fn: F) -> Self {
        DataWithQuery { data, query_fn }
    }
}

impl<Item, F> DbCleanable for DataWithQuery<Item, F>
where
    Item: field_count::FieldCount
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Clone,
    F: Fn(
            Cow<'static, [Item]>,
            PgDbPool,
        ) -> Pin<Box<dyn Future<Output = QueryResult<usize>> + Send + 'static>>
        + Send
        + Sync
        + Copy
        + 'static,
{
    fn clean(&mut self) {
        self.data = self.data.iter().map(remove_null_bytes).collect()
    }
}

impl<Item, F> DbRowCountable for DataWithQuery<Item, F>
where
    Item: field_count::FieldCount
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Clone,
    F: Fn(
            Cow<'static, [Item]>,
            PgDbPool,
        ) -> Pin<Box<dyn Future<Output = QueryResult<usize>> + Send + 'static>>
        + Send
        + Sync
        + Copy
        + 'static,
{
    fn len(&self) -> usize {
        self.data.len()
    }
}

unsafe fn extend_lifetime<'a, T: ?Sized>(r: &'a T) -> &'static T {
    std::mem::transmute::<&'a T, &'static T>(r)
}

#[async_trait::async_trait]
impl<Item, F> DbExecutable for DataWithQuery<Item, F>
where
    Item: field_count::FieldCount
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Sync
        + Clone,
    F: Fn(
            Cow<'static, [Item]>,
            PgDbPool,
        ) -> Pin<Box<dyn Future<Output = QueryResult<usize>> + Send + 'static>>
        + Send
        + Sync
        + Copy
        + 'static,
{
    async fn execute_query(&self, conn: PgDbPool) -> QueryResult<usize> {
        (self.query_fn)(Cow::Borrowed(&self.data), conn).await
    }
}
