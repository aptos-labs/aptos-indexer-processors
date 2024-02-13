use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;
use std::any::Any;

pub const DEFAULT_QUERY_CHUNK_SIZE: usize = 10;

#[derive(QueryId)]
/// Using this will append a where clause at the end of the string upsert function, e.g.
/// INSERT INTO ... ON CONFLICT DO UPDATE SET ... WHERE "transaction_version" = excluded."transaction_version"
/// This is needed when we want to maintain a table with only the latest state
pub struct QueryModelFragment {
    pub query: Box<dyn QueryFragment<Pg> + Send>,
    pub additional_where_clause: Option<&'static str>,
}

pub trait QueryModelTrait:
    Send + Sync + serde::Serialize + for<'de> serde::Deserialize<'de> + Clone
{
    fn as_any(&self) -> &dyn Any;
}

pub trait QueryModelBatchTrait: Clone + Send + Sync {
    fn items_to_insert(&self) -> Vec<&dyn QueryModelTrait>;

    fn chunk_size(&self) -> usize {
        DEFAULT_QUERY_CHUNK_SIZE
    }

    fn build_query(&self, items_to_insert: Vec<&dyn QueryModelTrait>) -> QueryModelFragment;

    fn build_query_fn<U>(&self) -> fn(Vec<&dyn QueryModelTrait>, bool) -> (U, Option<&'static str>)
    where
        U: QueryFragment<Pg> + diesel::query_builder::QueryId + Send;
}
