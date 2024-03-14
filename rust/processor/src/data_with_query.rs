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
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, future::Future, pin::Pin};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestModel {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestDb {}

pub struct DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    pub build_query_fn: fn(&'a [Item]) -> Query,
    pub items: Vec<Item>,
}

impl<'a, Query, Item> DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    pub fn new(items: Vec<Item>, build_query_fn: fn(&'a [Item]) -> Query) -> Self {
        Self {
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

impl<'a, Query, Item> DbCleanable for DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    fn clean(&mut self) {
        self.items = self.items.iter().map(remove_null_bytes).collect()
    }
}

impl<'a, Query, Item> DbRowCountable for DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    fn len(&self) -> usize {
        self.items.len()
    }
}

unsafe fn extend_lifetime<'a, T: ?Sized>(r: &'a T) -> &'static T {
    std::mem::transmute::<&'a T, &'static T>(r)
}

#[async_trait::async_trait]
impl<'a, Query, Item> DbExecutable for DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone,
{
    async fn execute_query(&self, conn: PgDbPool) -> QueryResult<usize> {
        //let items_ref: &'a [Item] = &items;
        let items_ref = unsafe { extend_lifetime(&self.items) };
        let query = (self.build_query_fn)(items_ref);
        drop(query);
        Ok(0)
    }
}
