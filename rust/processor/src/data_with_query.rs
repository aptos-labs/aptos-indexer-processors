/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{
    db_writer::{DbCleanable, DbExecutable, DbRowCountable},
    utils::{database::PgDbPool, util::remove_null_bytes},
};
use diesel::{query_builder::QueryFragment, QueryResult};
use std::pin::Pin;

/*
pub trait GenQueryFun<Item, F, Query>: Copy
where
    Query: for<'a> QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    fn generate_query(&self, items: &[Item]) -> Query;
}

impl<Item, F, Query> GenQueryFun<Item, F, Query> for F
where
    Query: for<'a> QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'static,
    F: Fn(&[Item]) -> Query + Send + Sync + Copy + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    fn generate_query(&self, items: &[Item]) -> Query {
        (self)(items)
    }
}*/

pub struct DataWithQuery<Item, F, Query>
where
    Query: for<'a> QueryFragment<diesel::pg::Pg>
        + diesel::query_builder::QueryId
        + Send
        + Sync
        + 'static,
    F: Fn(&[Item]) -> Query + Send + Sync + Copy + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    pub build_query_fn: F,
    pub items: Pin<Box<Vec<Item>>>,
}

impl<Item, F, Query> DataWithQuery<Item, F, Query>
where
    Query: for<'a> QueryFragment<diesel::pg::Pg>
        + diesel::query_builder::QueryId
        + Send
        + Sync
        + 'static,
    F: Fn(&[Item]) -> Query + Send + Sync + Copy + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    pub fn new(items: Vec<Item>, build_query_fn: F) -> Self {
        Self {
            build_query_fn,
            items: Box::pin(items),
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

impl<Item, F, Query> DbCleanable for DataWithQuery<Item, F, Query>
where
    Query: for<'a> QueryFragment<diesel::pg::Pg>
        + diesel::query_builder::QueryId
        + Send
        + Sync
        + 'static,
    F: Fn(&[Item]) -> Query + Send + Sync + Copy + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    fn clean(&mut self) {
        let items = self.items.as_mut().iter().map(remove_null_bytes).collect();
        self.items = Box::pin(items);
    }
}

impl<Item, F, Query> DbRowCountable for DataWithQuery<Item, F, Query>
where
    Query: for<'a> QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync,
    F: Fn(&[Item]) -> Query + Send + Sync + Copy + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    fn len(&self) -> usize {
        self.items.len()
    }
}

unsafe fn extend_lifetime<'a, T: ?Sized>(r: &'a T) -> &'static T {
    std::mem::transmute::<&'a T, &'static T>(r)
}

#[async_trait::async_trait]
impl<Item, F, Query> DbExecutable for DataWithQuery<Item, F, Query>
where
    Query: for<'a> QueryFragment<diesel::pg::Pg>
        + diesel::query_builder::QueryId
        + Send
        + Sync
        + 'static,
    F: Fn(&[Item]) -> Query + Send + Sync + Copy + 'static,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    async fn execute_query(&self, conn: PgDbPool) -> QueryResult<usize> {
        let items_ref = unsafe { std::mem::transmute::<&[Item], &'static [Item]>(&self.items) };
        let query = (self.build_query_fn)(items_ref);
        drop(query);
        Ok(0)
    }
}
