use crate::processors::default_processor::TransactionModelBatch;
use crate::processors::EndVersion;
use crate::processors::StartVersion;
use crate::utils::database::get_chunks;
use crate::utils::database::UpsertFilterLatestTransactionQuery;
use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;
use diesel::BoxableExpression;
use enum_dispatch::enum_dispatch;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;

pub const DEFAULT_QUERY_CHUNK_SIZE: usize = 10;

#[derive(QueryId)]
/// Using this will append a where clause at the end of the string upsert function, e.g.
/// INSERT INTO ... ON CONFLICT DO UPDATE SET ... WHERE "transaction_version" = excluded."transaction_version"
/// This is needed when we want to maintain a table with only the latest state
pub struct QueryModelFragment {
    pub query: Box<dyn QueryFragment<Pg>>,
    pub where_clause: Option<&'static str>,
}

pub trait QueryModelBatchTrait: Send {
    fn build_query(&self) -> QueryModelFragment;
}
