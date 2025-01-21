use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::stake_models::proposal_voters::{
        RawProposalVote, RawProposalVoteConvertible,
    },
};
use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ProposalVote {
    pub transaction_version: i64,
    pub proposal_id: i64,
    pub voter_address: String,
    pub staking_pool_address: String,
    pub num_votes: String, // BigDecimal
    pub should_pass: bool,
    #[allocative(skip)]
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ProposalVote {
    const TABLE_NAME: &'static str = "proposal_votes";
}

impl HasVersion for ProposalVote {
    fn version(&self) -> i64 {
        self.transaction_version
    }
}

impl GetTimeStamp for ProposalVote {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.transaction_timestamp
    }
}

impl RawProposalVoteConvertible for ProposalVote {
    fn from_raw(raw: RawProposalVote) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            proposal_id: raw.proposal_id,
            voter_address: raw.voter_address,
            staking_pool_address: raw.staking_pool_address,
            num_votes: raw.num_votes.to_string(),
            should_pass: raw.should_pass,
            transaction_timestamp: raw.transaction_timestamp,
        }
    }
}
