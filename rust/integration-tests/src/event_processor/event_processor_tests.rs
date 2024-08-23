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
        BoolExpressionMethods, ExpressionMethods, QueryResult, RunQueryDsl,
    };
    use processor::schema::events::dsl::*;

    const WITHDRAW_EVENT: &str = "0x1::coin::WithdrawEvent";
    const DEPOSIT_EVENT: &str = "0x1::coin::DepositEvent";
    const FEE_STATEMENT_EVENT: &str = "0x1::transaction_fee::FeeStatement";
    const DISTRIBUTE_REWARDS_EVENT: &str = "0x1::stake::DistributeRewardsEvent";

    // Test Case: Validate the transaction that funds an account A from the faucet parsed into the correct events.
    // - Verifies that the faucet funding results in one WithdrawEvent and two DepositEvents.
    #[tokio::test]
    async fn test_faucet_funding_transaction_with_correct_events() {
        let test_context = TestContext::new(&[ACCOUNT_A_GET_FUNDED_FROM_FAUCET])
            .await
            .unwrap();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        let expected_transaction = test_context.transaction_batches[0].clone();

        assert!(test_context
            .run(processor_config, move |conn: &mut PgConnection| {
                // Load and validate events
                let withdraw_events = load_transaction_events(conn, 0, WITHDRAW_EVENT)
                    .expect("Failed to load WithdrawEvent");
                assert_eq!(withdraw_events.len(), 1);

                validate_fee_statement_event(
                    &withdraw_events[0],
                    expected_transaction.version as i64,
                    "0x832fd5e456b7f43c4ef27978766ee5242a8a393d10ef5bf662d40f774a6f2deb",
                    3,
                    WITHDRAW_EVENT,
                );

                let deposit_events = load_transaction_events(conn, 0, DEPOSIT_EVENT)
                    .expect("Failed to load DepositEvent");
                assert_eq!(deposit_events.len(), 2);

                validate_fee_statement_event(
                    &deposit_events[0],
                    expected_transaction.version as i64,
                    "0x832fd5e456b7f43c4ef27978766ee5242a8a393d10ef5bf662d40f774a6f2deb",
                    2,
                    DEPOSIT_EVENT,
                );

                Ok(())
            })
            .await
            .is_ok());
    }

    // Test Case: Validate that a validator transaction generates the correct events.
    // - Verifies that a validator transaction results in 20 events, with 19 being DistributeRewardsEvents.
    #[tokio::test]
    async fn test_validator_transaction_distribute_rewards_events() {
        let test_context = TestContext::new(&[VALIDATOR_TRANSACTION]).await.unwrap();
        let expected_transaction = test_context.transaction_batches[0].clone();

        assert!(test_context
            .run(
                default_processor_config(),
                move |conn: &mut PgConnection| {
                    // Load and validate events
                    let distributed_rewards_events =
                        load_transaction_events(conn, 0, DISTRIBUTE_REWARDS_EVENT)
                            .expect("Failed to load DistributeRewardsEvents");
                    assert_eq!(distributed_rewards_events.len(), 19);

                    validate_fee_statement_event(
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
        let test_context = TestContext::new(&[GENERATED_USER_SCRIPT_TRANSACTION])
            .await
            .unwrap();
        let expected_transaction = test_context.transaction_batches[0].clone();

        assert!(test_context
            .run(
                default_processor_config(),
                move |conn: &mut PgConnection| {
                    // Load and validate events
                    let actual_events = load_transaction_events(conn, 0, FEE_STATEMENT_EVENT)
                        .expect("Failed to load FeeStatement events");
                    assert_eq!(actual_events.len(), 1);

                    validate_fee_statement_event(
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

    // Test Case: Validate that transferring funds between accounts transactions parsed into the correct events.
    // - Verifies that the sequence of transfers results in three WithdrawEvents (one from faucet, one from Account A, one from Account B).
    #[tokio::test]
    async fn test_multiple_account_transfers_with_correct_withdraw_events() {
        let test_context = TestContext::new(&[
            ACCOUNT_A_GET_FUNDED_FROM_FAUCET,
            ACCOUNT_A_TRANSFER_TO_ACCOUNT_B,
            ACCOUNT_B_TRANSFER_TO_ACCOUNT_B,
        ])
        .await
        .unwrap();

        assert!(test_context
            .run(default_processor_config(), |conn: &mut PgConnection| {
                let result = events
                    .select((
                        transaction_version,
                        event_index,
                        type_,
                        data,
                        creation_number,
                    ))
                    .filter(transaction_version.eq(0))
                    .load::<(i64, i64, String, serde_json::Value, i64)>(conn);
                let result_events = result.expect("Failed to load events");
                assert_eq!(result_events.len(), 5);

                let withdraw_events = events
                    .select((transaction_version, event_index, type_))
                    .filter(type_.eq("0x1::coin::WithdrawEvent"))
                    .load::<(i64, i64, String)>(conn);

                // Withdraw from Faucet, Withdraw from AccountA, Withdraw from AccountB
                assert_eq!(withdraw_events.unwrap().len(), 3);

                let deposit_events = events
                    .select((
                        transaction_version,
                        event_index,
                        type_,
                        data,
                        account_address,
                    ))
                    .filter(type_.eq("0x1::coin::DepositEvent"))
                    .load::<(i64, i64, String, serde_json::Value, String)>(conn);

                let deposit_events = deposit_events.expect("Failed to load DepositEvent");
                let account_a_address =
                    "0xeea01e0c163fe390e30afd0e6ff88a3535a2d78b4cbbcc8f9bf3848b5a8fbcf2";
                // Verify that specific events with a given transaction version and creation number contain the correct data
                deposit_events
                    .iter()
                    .filter(|(_, _, _, _, _account_address)| _account_address.eq(account_a_address)) // Ensure correct creation number
                    .for_each(|(_, _, _, _data, _)| {
                        // Ensure that any key starting with "amount" has the expected value
                        _data
                            .as_object()
                            .unwrap()
                            .iter()
                            .filter(|(k, _)| k.starts_with("amount"))
                            .for_each(|(_, v)| {
                                assert_eq!(v.as_str().unwrap(), "100000000"); // checking deposit into account A is 100000000
                            });
                    });

                // total 4 deposit events = 2 from faucet, 1 from account A, and 1 from account b
                assert_eq!(deposit_events.len(), 4);

                Ok(())
            })
            .await
            .is_ok());
    }

    // Default Event processor configuration
    // TODO: Move this to a common location later when we have more processor tests
    fn default_processor_config() -> TestProcessorConfig {
        TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        }
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

    fn validate_fee_statement_event(
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
