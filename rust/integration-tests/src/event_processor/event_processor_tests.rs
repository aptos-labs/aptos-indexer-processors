#[cfg(test)]
mod test {
    use crate::{TestContext, TestProcessorConfig};
    use aptos_indexer_test_transactions::{
        ACCOUNT_A_GET_FUNDED_FROM_FAUCET, ACCOUNT_A_TRANSFER_TO_ACCOUNT_B,
        ACCOUNT_B_TRANSFER_TO_ACCOUNT_B, GENERATED_USER_SCRIPT_TRANSACTION, VALIDATOR_TRANSACTION,
    };
    use diesel::{
        pg::PgConnection,
        query_dsl::methods::{FilterDsl, SelectDsl},
        ExpressionMethods, RunQueryDsl,
    };
    use processor::schema::events::dsl::*;

    #[tokio::test]
    async fn test_faucet_funded_event() {
        let test_context = TestContext::new(&[ACCOUNT_A_GET_FUNDED_FROM_FAUCET])
            .await
            .unwrap();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        assert!(test_context
            .run(processor_config, |conn: &mut PgConnection| {
                let result = events
                    .select((transaction_version, event_index, type_))
                    .filter(transaction_version.eq(0))
                    .load::<(i64, i64, String)>(conn);
                assert_eq!(result.unwrap().len(), 5);

                let withdraw_events = events
                    .select((transaction_version, event_index, type_))
                    .filter(type_.eq("0x1::coin::WithdrawEvent"))
                    .load::<(i64, i64, String)>(conn);

                // Withdraw from Faucet, Withdraw from AccountA, Withdraw from AccountB
                assert_eq!(withdraw_events.unwrap().len(), 1);

                let deposit_events = events
                    .select((transaction_version, event_index, type_))
                    .filter(type_.eq("0x1::coin::DepositEvent"))
                    .load::<(i64, i64, String)>(conn);
                // Deposit to AccountA
                assert_eq!(deposit_events.unwrap().len(), 2);

                Ok(())
            })
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_transfer_event() {
        let test_context = TestContext::new(&[
            ACCOUNT_A_GET_FUNDED_FROM_FAUCET,
            ACCOUNT_A_TRANSFER_TO_ACCOUNT_B,
            ACCOUNT_B_TRANSFER_TO_ACCOUNT_B,
        ])
        .await
        .unwrap();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        assert!(test_context
            .run(processor_config, |conn: &mut PgConnection| {
                let result = events
                    .select((transaction_version, event_index, type_))
                    .filter(transaction_version.eq(0))
                    .load::<(i64, i64, String)>(conn);
                assert_eq!(result.unwrap().len(), 5);

                let withdraw_events = events
                    .select((transaction_version, event_index, type_))
                    .filter(type_.eq("0x1::coin::WithdrawEvent"))
                    .load::<(i64, i64, String)>(conn);

                // Withdraw from Faucet, Withdraw from AccountA, Withdraw from AccountB
                assert_eq!(withdraw_events.unwrap().len(), 3);
                Ok(())
            })
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validator_txn_events() {
        let test_context = TestContext::new(&[VALIDATOR_TRANSACTION]).await.unwrap();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };

        assert!(test_context
            .run(processor_config, |conn: &mut PgConnection| {
                let result = events
                    .select((transaction_version, event_index, type_))
                    .filter(transaction_version.eq(0))
                    .load::<(i64, i64, String)>(conn);
                assert_eq!(result.unwrap().len(), 20);

                let distributed_rewards_events = events
                    .select((transaction_version, event_index, type_))
                    .filter(type_.eq("0x1::stake::DistributeRewardsEvent"))
                    .load::<(i64, i64, String)>(conn);
                assert_eq!(distributed_rewards_events.unwrap().len(), 19);
                Ok(())
            })
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_generated_user_script_events() {
        let test_context = TestContext::new(&[GENERATED_USER_SCRIPT_TRANSACTION])
            .await
            .unwrap();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        assert!(test_context
            .run(processor_config, |conn: &mut PgConnection| {
                let result = events
                    .select((transaction_version, event_index, type_))
                    .filter(transaction_version.eq(0))
                    .load::<(i64, i64, String)>(conn);
                assert_eq!(result.unwrap().len(), 1);

                let fee_statement_events = events
                    .select((transaction_version, event_index, type_))
                    .filter(type_.eq("0x1::transaction_fee::FeeStatement"))
                    .load::<(i64, i64, String)>(conn);
                assert_eq!(fee_statement_events.unwrap().len(), 1);
                Ok(())
            })
            .await
            .is_ok());
    }
}
