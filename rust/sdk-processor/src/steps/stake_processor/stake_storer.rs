use crate::{
    processors::stake_processor::StakeProcessorConfig,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::postgres::models::stake_models::{
        current_delegated_voter::CurrentDelegatedVoter,
        delegator_activities::DelegatedStakingActivity,
        delegator_balances::{CurrentDelegatorBalance, DelegatorBalance},
        delegator_pools::{CurrentDelegatorPoolBalance, DelegatorPool, DelegatorPoolBalance},
        proposal_votes::ProposalVote,
        staking_pool_voter::CurrentStakingPoolVoter,
    },
    processors::stake_processor::{
        insert_current_delegated_voter_query, insert_current_delegator_balances_query,
        insert_current_delegator_pool_balances_query, insert_current_stake_pool_voter_query,
        insert_delegator_activities_query, insert_delegator_balances_query,
        insert_delegator_pool_balances_query, insert_delegator_pools_query,
        insert_proposal_votes_query,
    },
};

pub struct StakeStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: StakeProcessorConfig,
}

impl StakeStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: StakeProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for StakeStorer {
    type Input = (
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
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
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
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let per_table_chunk_sizes: AHashMap<String, usize> = self
            .processor_config
            .default_config
            .per_table_chunk_sizes
            .clone();

        let (
            current_stake_pool_voters,
            proposal_votes,
            delegator_activities,
            delegator_balances,
            current_delegator_balances,
            delegator_pools,
            delegator_pool_balances,
            current_delegator_pool_balances,
            current_delegated_voter,
        ) = input.data;

        let cspv = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_stake_pool_voter_query,
            &current_stake_pool_voters,
            get_config_table_chunk_size::<CurrentStakingPoolVoter>(
                "current_staking_pool_voter",
                &per_table_chunk_sizes,
            ),
        );
        let pv = execute_in_chunks(
            self.conn_pool.clone(),
            insert_proposal_votes_query,
            &proposal_votes,
            get_config_table_chunk_size::<ProposalVote>("proposal_votes", &per_table_chunk_sizes),
        );
        let da = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_activities_query,
            &delegator_activities,
            get_config_table_chunk_size::<DelegatedStakingActivity>(
                "delegated_staking_activities",
                &per_table_chunk_sizes,
            ),
        );
        let db = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_balances_query,
            &delegator_balances,
            get_config_table_chunk_size::<DelegatorBalance>(
                "delegator_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cdb = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_delegator_balances_query,
            &current_delegator_balances,
            get_config_table_chunk_size::<CurrentDelegatorBalance>(
                "current_delegator_balances",
                &per_table_chunk_sizes,
            ),
        );
        let dp = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_pools_query,
            &delegator_pools,
            get_config_table_chunk_size::<DelegatorPool>(
                "delegated_staking_pools",
                &per_table_chunk_sizes,
            ),
        );
        let dpb = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_pool_balances_query,
            &delegator_pool_balances,
            get_config_table_chunk_size::<DelegatorPoolBalance>(
                "delegated_staking_pool_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cdpb = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_delegator_pool_balances_query,
            &current_delegator_pool_balances,
            get_config_table_chunk_size::<CurrentDelegatorPoolBalance>(
                "current_delegated_staking_pool_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cdv = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_delegated_voter_query,
            &current_delegated_voter,
            get_config_table_chunk_size::<CurrentDelegatedVoter>(
                "current_delegated_voter",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(cspv, pv, da, db, cdb, dp, dpb, cdpb, cdv)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for StakeStorer {}

impl NamedStep for StakeStorer {
    fn name(&self) -> String {
        "StakeStorer".to_string()
    }
}
