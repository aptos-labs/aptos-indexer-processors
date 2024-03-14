/***
   The goal of this module is to move db writes to their own executor pool
   This allows us to have a separate pool of threads for db writes
   In the future, we'd like to use a rayon pool for the core processing, and isolate async code
*/

use crate::{
    db_writer::{execute_with_better_error, DbCleanable, DbExecutable, DbRowCountable},
    utils::{database::PgDbPool, util::remove_null_bytes},
};
use diesel::{query_builder::QueryFragment, QueryResult};
use std::pin::Pin;

pub struct DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    pub build_query_fn: fn(&'a [Item]) -> Query,
    pub items: Pin<Box<Vec<Item>>>,
}

impl<'a, Query, Item> DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    pub fn new(items: Vec<Item>, build_query_fn: fn(&'a [Item]) -> Query) -> Self {
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

impl<'a, Query, Item> DbCleanable for DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    fn clean(&mut self) {
        let items = self.items.as_mut().iter().map(remove_null_bytes).collect();
        self.items = Box::pin(items);
    }
}

impl<'a, Query, Item> DbRowCountable for DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    fn len(&self) -> usize {
        self.items.len()
    }
}

#[async_trait::async_trait]
impl<'a, Query, Item> DbExecutable for DataWithQuery<'a, Query, Item>
where
    Query: QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Send + Sync + 'a,
    Item: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    /**
    This function will execute the query with the given db pool

    The unsafe block is used to extend the lifetime of `&[Item]` from whatever it originally is to `'a`.
    This is dangerous as it fundamentally alters Rust's guarantees about lifetimes and memory safety.
    We could not find a way of convincing the compiler in a safe way.

    It can be safe if and only if the following conditions are met:
    1. *Actual Lifetime Matches or Exceeds `'a`*: The actual lifetime of `self.items` must be at least as long as `'a`.
        This is crucial because we the compiler that `items_ref` is valid for the lifetime `'a`.
        If `self.items` does not live as long as `'a`, then `items_ref` could become a dangling reference, leading to undefined behavior.
    2. *No Mutable Aliasing*: Since `items_ref` is an immutable reference, it must be ensured that no mutable references to `self.items` exist for the duration of `'a`,
    as this could lead to data races in a multithreaded context.

    Since `'a` is tied to the `DataWithQuery` struct itself, and the struct owns the `self.items`, this should be safe.
    Additionally, `self.items` is a `Pin<Box<Vec<Item>>>`, so it's location in memory should never change. This may not be necessary.

    *This would not be safe* if the `DataWithQuery` instance (and by extension `self.items`) is dropped while `items_ref` is still in use.
    This could happen if `execute_query` is called, and then the `DataWithQuery` instance is dropped before the future returned by `execute_query` is polled to completion.
    Rusts borrow checker _*should*_ prevent this from happening, i.e the following example is unsafe and would be very bad, except
    the borrow checker will prevent it from compiling:
    ```text
    134 |     let data_with_query = DataWithQuery::new(vec![], insert_events_query);
        |         --------------- binding `data_with_query` declared here
    135 |     let future = data_with_query.execute_query(db_pool);
        |                  --------------- borrow of `data_with_query` occurs here
    ```

    THIS WOULD BE UNSAFE, IF IT COMPILED:
    ```rust
    use processor::{utils::database, data_with_query::DataWithQuery, db_writer::DbExecutable};
    use processor::processors::events_processor::insert_events_query;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unsafe_example() {
        let db_pool = database::new_db_pool(&"postgres://...", 5)
            .await
            .expect("Failed to create connection pool");

        let data_with_query = DataWithQuery::new(vec![], insert_events_query);
        let future = data_with_query.execute_query(db_pool);

        // Drop data_with_query immediately after calling execute_query
        drop(data_with_query);

        // If the future is polled after data_with_query is dropped, items_ref could be a dangling reference
        let _ = future.await;
    }
    ```
    */

    async fn execute_query(&self, db_pool: PgDbPool) -> QueryResult<usize> {
        let items_ref = unsafe { std::mem::transmute::<&[Item], &'a [Item]>(&self.items) };
        let query = (self.build_query_fn)(items_ref);
        execute_with_better_error(db_pool, query).await
    }
}
