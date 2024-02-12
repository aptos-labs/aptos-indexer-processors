use crate::processors::default_processor::TransactionModelBatch;
use crate::processors::EndVersion;
use crate::processors::StartVersion;
use crate::utils::database::get_chunks;
use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;
use enum_dispatch::enum_dispatch;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;

pub const DEFAULT_QUERY_CHUNK_SIZE: usize = 10;

#[enum_dispatch]
pub trait QueryModelBatchTrait: Send {
    fn build_query(
        &self,
    ) -> (
        impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
        Option<&'static str>,
    );
}

#[enum_dispatch(QueryModelBatchTrait)]
#[derive(Clone, Debug)]
pub enum QueryModelBatch {
    // BlockMetadataTransactionModel,
    TransactionModelBatch,
}
