#[cfg(test)]
mod test {
    use crate::{TestContext, TestProcessorConfig};
    use aptos_indexer_test_transactions::{
        ACCOUNT_A_GET_FUNDED_FROM_FAUCET, ACCOUNT_A_TRANSFER_TO_ACCOUNT_B,
        ACCOUNT_B_TRANSFER_TO_ACCOUNT_B,
    };
    use diesel::{
        pg::PgConnection,
        query_dsl::methods::{FilterDsl, SelectDsl},
        ExpressionMethods, RunQueryDsl,
    };
    use processor::schema::events::dsl::*;

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
}
