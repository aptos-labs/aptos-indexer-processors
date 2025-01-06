use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    utils::parquet_extractor_helper::add_to_map_if_opted_in_for_backfill,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::{
        common::models::stake_models::{
            delegator_activities::RawDelegatedStakingActivityConvertible,
            delegator_balances::{
                RawCurrentDelegatorBalanceConvertible, RawDelegatorBalanceConvertible,
            },
            proposal_voters::RawProposalVoteConvertible,
        },
        parquet::models::stake_models::{
            parquet_delegator_activities::DelegatedStakingActivity,
            parquet_delegator_balances::{CurrentDelegatorBalance, DelegatorBalance},
            parquet_proposal_voters::ProposalVote,
        },
    },
    processors::stake_processor::parse_stake_data,
    utils::table_flags::TableFlags,
};
use std::collections::HashMap;
use tracing::{debug, error};

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetStakeExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetStakeExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (
            _,
            raw_all_proposal_votes,
            raw_all_delegator_activities,
            raw_all_delegator_balances,
            raw_all_current_delegator_balances,
            _,
            _,
            _,
            _,
        ) = match parse_stake_data(&transactions.data, None, 0, 0).await {
            Ok(data) => data,
            Err(e) => {
                error!(
                    start_version = transactions.metadata.start_version,
                    end_version = transactions.metadata.end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error parsing stake data",
                );
                return Err(ProcessorError::ProcessError {
                    message: format!("Error parsing stake data: {:?}", e),
                });
            },
        };

        let all_delegator_activities = raw_all_delegator_activities
            .into_iter()
            .map(DelegatedStakingActivity::from_raw)
            .collect::<Vec<_>>();
        let all_delegator_balances: Vec<DelegatorBalance> = raw_all_delegator_balances
            .into_iter()
            .map(DelegatorBalance::from_raw)
            .collect::<Vec<_>>();
        let all_current_delegator_balances = raw_all_current_delegator_balances
            .into_iter()
            .map(CurrentDelegatorBalance::from_raw)
            .collect::<Vec<_>>();
        let all_proposal_votes = raw_all_proposal_votes
            .into_iter()
            .map(ProposalVote::from_raw)
            .collect::<Vec<_>>();

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(
            " - DelegatedStakingActivity: {}",
            all_delegator_activities.len()
        );
        debug!(" - ProposalVote: {}", all_proposal_votes.len());
        debug!(" - DelegatorBalance: {}", all_delegator_balances.len());
        debug!(
            " - CurrentDelegatorBalance: {}",
            all_current_delegator_balances.len()
        );

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        let data_types = [
            (
                TableFlags::DELEGATED_STAKING_ACTIVITIES,
                ParquetTypeEnum::DelegatedStakingActivities,
                ParquetTypeStructs::DelegatedStakingActivity(all_delegator_activities),
            ),
            (
                TableFlags::PROPOSAL_VOTES,
                ParquetTypeEnum::ProposalVotes,
                ParquetTypeStructs::ProposalVote(all_proposal_votes),
            ),
            (
                TableFlags::DELEGATOR_BALANCES,
                ParquetTypeEnum::DelegatorBalances,
                ParquetTypeStructs::DelegatorBalance(all_delegator_balances),
            ),
            (
                TableFlags::CURRENT_DELEGATOR_BALANCES,
                ParquetTypeEnum::CurrentDelegatorBalances,
                ParquetTypeStructs::CurrentDelegatorBalance(all_current_delegator_balances),
            ),
        ];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetStakeExtractor {}

impl NamedStep for ParquetStakeExtractor {
    fn name(&self) -> String {
        "ParquetStakeExtractor".to_string()
    }
}
