use crate::utils::database::ArcDbPool;
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
            current_delegated_voter::CurrentDelegatedVoter,
            delegator_activities::RawDelegatedStakingActivityConvertible,
            delegator_balances::{
                RawCurrentDelegatorBalanceConvertible, RawDelegatorBalanceConvertible,
            },
            delegator_pools::{
                DelegatorPool, RawCurrentDelegatorPoolBalanceConvertible,
                RawDelegatorPoolBalanceConvertible,
            },
            proposal_voters::RawProposalVoteConvertible,
            staking_pool_voter::RawCurrentStakingPoolVoterConvertible,
        },
        postgres::models::stake_models::{
            delegator_activities::DelegatedStakingActivity,
            delegator_balances::{CurrentDelegatorBalance, DelegatorBalance},
            delegator_pools::{CurrentDelegatorPoolBalance, DelegatorPoolBalance},
            proposal_votes::ProposalVote,
            staking_pool_voter::CurrentStakingPoolVoter,
        },
    },
    processors::stake_processor::parse_stake_data,
};
use tracing::error;

pub struct StakeExtractor
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    query_retries: u32,
    query_retry_delay_ms: u64,
}

impl StakeExtractor {
    pub fn new(conn_pool: ArcDbPool, query_retries: u32, query_retry_delay_ms: u64) -> Self {
        Self {
            conn_pool,
            query_retries,
            query_retry_delay_ms,
        }
    }
}

#[async_trait]
impl Processable for StakeExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<CurrentStakingPoolVoter>,
        Vec<ProposalVote>,
        Vec<DelegatedStakingActivity>,
        Vec<DelegatorBalance>,
        Vec<CurrentDelegatorBalance>,
        Vec<DelegatorPool>,
        Vec<DelegatorPoolBalance>,
        Vec<CurrentDelegatorPoolBalance>,
        Vec<CurrentDelegatedVoter>,
    );
    type RunType = AsyncRunType;

    /// Processes a batch of transactions and extracts relevant staking data.
    ///
    /// This function processes a batch of transactions, extracting various types of staking-related
    /// data such as current staking pool voters, proposal votes, delegated staking activities,
    /// delegator balances, and more. The extracted data is then returned in a `TransactionContext`
    /// for further processing or storage.
    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<CurrentStakingPoolVoter>,
                Vec<ProposalVote>,
                Vec<DelegatedStakingActivity>,
                Vec<DelegatorBalance>,
                Vec<CurrentDelegatorBalance>,
                Vec<DelegatorPool>,
                Vec<DelegatorPoolBalance>,
                Vec<CurrentDelegatorPoolBalance>,
                Vec<CurrentDelegatedVoter>,
            )>,
        >,
        ProcessorError,
    > {
        let conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get connection from pool: {:?}", e),
                query: None,
            })?;

        let (
            raw_all_current_stake_pool_voters,
            raw_all_proposal_votes,
            raw_all_delegator_activities,
            raw_all_delegator_balances,
            raw_all_current_delegator_balances,
            all_delegator_pools,
            raw_all_delegator_pool_balances,
            raw_all_current_delegator_pool_balances,
            all_current_delegated_voter,
        ) = match parse_stake_data(
            &transactions.data,
            Some(conn),
            self.query_retries,
            self.query_retry_delay_ms,
        )
        .await
        {
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

        let all_delegator_balances: Vec<DelegatorBalance> = raw_all_delegator_balances
            .into_iter()
            .map(DelegatorBalance::from_raw)
            .collect::<Vec<_>>();
        let all_current_delegator_balances = raw_all_current_delegator_balances
            .into_iter()
            .map(CurrentDelegatorBalance::from_raw)
            .collect::<Vec<_>>();
        let all_delegator_pool_balances = raw_all_delegator_pool_balances
            .into_iter()
            .map(DelegatorPoolBalance::from_raw)
            .collect::<Vec<_>>();
        let all_current_delegator_pool_balances = raw_all_current_delegator_pool_balances
            .into_iter()
            .map(CurrentDelegatorPoolBalance::from_raw)
            .collect::<Vec<_>>();
        let all_delegator_activities = raw_all_delegator_activities
            .into_iter()
            .map(DelegatedStakingActivity::from_raw)
            .collect::<Vec<_>>();
        let all_proposal_votes = raw_all_proposal_votes
            .into_iter()
            .map(ProposalVote::from_raw)
            .collect::<Vec<_>>();
        let all_current_stake_pool_voters = raw_all_current_stake_pool_voters
            .into_iter()
            .map(CurrentStakingPoolVoter::from_raw)
            .collect::<Vec<_>>();

        Ok(Some(TransactionContext {
            data: (
                all_current_stake_pool_voters,
                all_proposal_votes,
                all_delegator_activities,
                all_delegator_balances,
                all_current_delegator_balances,
                all_delegator_pools,
                all_delegator_pool_balances,
                all_current_delegator_pool_balances,
                all_current_delegated_voter,
            ),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for StakeExtractor {}

impl NamedStep for StakeExtractor {
    fn name(&self) -> String {
        "StakeExtractor".to_string()
    }
}
