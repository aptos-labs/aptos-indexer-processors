#[allow(clippy::needless_return)]
#[cfg(test)]
mod test {
    use crate::{ScenarioTest, TestContext, TestProcessorConfig, TestType};
    use aptos_indexer_test_transactions::{
        IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
        IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
        IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
    };
    use diesel::{
        pg::PgConnection,
        query_dsl::methods::{FilterDsl, SelectDsl},
        BoolExpressionMethods, ExpressionMethods, QueryResult, RunQueryDsl,
    };
    use processor::schema::events::dsl::*;

    const FA_WITHDRAW_EVENT: &str = "0x1::fungible_asset::Withdraw";
    const FA_DEPOSIT_EVENT: &str = "0x1::fungible_asset::Deposit";
    const FEE_STATEMENT_EVENT: &str = "0x1::transaction_fee::FeeStatement";
    const DISTRIBUTE_REWARDS_EVENT: &str = "0x1::stake::DistributeRewardsEvent";

    // Test Case: Validate the transaction that funds an account A from the faucet parsed into the correct events.
    // - Verifies that the faucet funding results in one WithdrawEvent and two DepositEvents.
    #[tokio::test]
    async fn test_fungible_asset_withdraw_deposit_events() {
        let test_context = TestContext::new(&[IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES])
            .await
            .unwrap();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        let expected_transaction = test_context.transaction_batches[0].clone();
        let test_type = TestType::Scenario(ScenarioTest);

        assert!(test_context
            .run(
                processor_config,
                test_type,
                move |conn: &mut PgConnection, version: &str| {
                    // Load and validate events
                    let withdraw_events = load_transaction_events(
                        conn,
                        version.parse::<i64>().unwrap(),
                        FA_WITHDRAW_EVENT,
                    )
                    .expect("Failed to load WithdrawEvent");
                    assert_eq!(withdraw_events.len(), 1);

                    validate_event(
                        &withdraw_events[0],
                        expected_transaction.version as i64,
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        0,
                        FA_WITHDRAW_EVENT,
                    );

                    let deposit_events = load_transaction_events(
                        conn,
                        version.parse::<i64>().unwrap(),
                        FA_DEPOSIT_EVENT,
                    )
                    .expect("Failed to load DepositEvent");
                    assert_eq!(deposit_events.len(), 1);

                    validate_event(
                        &deposit_events[0],
                        expected_transaction.version as i64,
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        0,
                        FA_DEPOSIT_EVENT,
                    );

                    Ok(())
                }
            )
            .await
            .is_ok());
    }

    // Test Case: Validate that a validator transaction generates the correct events.
    // - Verifies that a validator transaction results in 20 events, with 19 being DistributeRewardsEvents.
    #[tokio::test]
    async fn test_validaator_distribute_rewards_events() {
        let test_context = TestContext::new(&[IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN])
            .await
            .unwrap();
        let expected_transaction = test_context.transaction_batches[0].clone();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        let test_type = TestType::Scenario(ScenarioTest);
        assert!(test_context
            .run(
                processor_config,
                test_type,
                move |conn: &mut PgConnection, version: &str| {
                    // Load and validate events
                    let distributed_rewards_events = load_transaction_events(
                        conn,
                        version.parse::<i64>().unwrap(),
                        DISTRIBUTE_REWARDS_EVENT,
                    )
                    .expect("Failed to load DistributeRewardsEvents");
                    assert_eq!(distributed_rewards_events.len(), 19);

                    validate_event(
                        &distributed_rewards_events[0],
                        expected_transaction.version as i64,
                        "0x0a4113560d0b18ba38797f2a899c4b27e0c5b0476be5d8f6be68fba8b1861ed0",
                        12,
                        DISTRIBUTE_REWARDS_EVENT,
                    );

                    Ok(())
                }
            )
            .await
            .is_ok());
    }

    // Test Case: Validate that a user-generated script transaction generates the correct event.
    // - Verifies that a transaction using a user-defined script results in exactly one FeeStatement event.
    #[tokio::test]
    async fn test_user_script_transaction_fee_statement_event() {
        let test_context = TestContext::new(&[IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER])
            .await
            .unwrap();
        let expected_transaction = test_context.transaction_batches[0].clone();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        let test_type = TestType::Scenario(ScenarioTest);
        assert!(test_context
            .run(
                processor_config,
                test_type,
                move |conn: &mut PgConnection, version: &str| {
                    // Load and validate events
                    let actual_events = load_transaction_events(
                        conn,
                        version.parse::<i64>().unwrap(),
                        FEE_STATEMENT_EVENT,
                    )
                    .expect("Failed to load FeeStatement events");
                    assert_eq!(actual_events.len(), 1);

                    validate_event(
                        &actual_events[0],
                        expected_transaction.version as i64,
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        0,
                        FEE_STATEMENT_EVENT,
                    );

                    Ok(())
                }
            )
            .await
            .is_ok());
    }

    fn load_transaction_events(
        conn: &mut PgConnection,
        txn_version: i64,
        event_type: &str,
    ) -> QueryResult<Vec<(i64, String, String, String, i64)>> {
        // Build the query with the required filters
        events
            .select((
                transaction_version,
                type_,
                indexed_type,
                account_address,
                creation_number,
            ))
            .filter(
                transaction_version
                    .eq(txn_version)
                    .and(type_.eq(event_type)),
            )
            .load::<(i64, String, String, String, i64)>(conn)
    }

    fn validate_event(
        actual_event: &(i64, String, String, String, i64),
        expected_transaction_version: i64,
        expected_account_address: &str,
        expected_creation_number: i64,
        expected_indexed_type: &str,
    ) {
        let (
            actual_transaction_version,
            actual_type,
            actual_indexed_type,
            actual_address,
            actual_creation_number,
        ) = actual_event;

        assert_eq!(*actual_transaction_version, expected_transaction_version);
        assert_eq!(actual_type, expected_indexed_type);
        assert_eq!(actual_indexed_type, &expected_indexed_type);
        assert_eq!(actual_address, expected_account_address);
        assert_eq!(actual_creation_number, &expected_creation_number);
    }
}
