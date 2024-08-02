#[cfg(test)]
mod test {
    use crate::{TestContext, TestProcessorConfig};
    use aptos_indexer_test_transactions::{
        ACCOUNT_A_GET_FUNDED_FROM_FAUCET, ACCOUNT_A_TRANSFER_TO_ACCOUNT_B,
        ACCOUNT_B_TRANSFER_TO_ACCOUNT_B,
    };
    use bigdecimal::BigDecimal;
    use diesel::{
        pg::PgConnection,
        query_dsl::methods::{FilterDsl, SelectDsl},
        ExpressionMethods, RunQueryDsl,
    };
    use processor::schema::current_coin_balances::dsl::*;

    #[tokio::test]
    async fn test_case_1() {
        let test_context = TestContext::new(&[
            ACCOUNT_A_GET_FUNDED_FROM_FAUCET,
            ACCOUNT_A_TRANSFER_TO_ACCOUNT_B,
            ACCOUNT_B_TRANSFER_TO_ACCOUNT_B,
        ])
        .await
        .unwrap();
        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::CoinProcessor,
        };
        assert!(test_context
            .run(processor_config, |conn: &mut PgConnection| {
                let result =
                    current_coin_balances
                        .select((owner_address, amount))
                        .filter(owner_address.eq(
                            "0xae9957b61de4c9e2c3a9d706d5785774b5c3ff365ecb692303b76ba849c4aea6",
                        ))
                        .load::<(String, BigDecimal)>(conn);
                assert_eq!(result.unwrap(), vec![(
                    "0xae9957b61de4c9e2c3a9d706d5785774b5c3ff365ecb692303b76ba849c4aea6"
                        .to_string(),
                    BigDecimal::from(49999200)
                )]);
                let result =
                    current_coin_balances
                        .select((owner_address, amount))
                        .filter(owner_address.eq(
                            "0xeea01e0c163fe390e30afd0e6ff88a3535a2d78b4cbbcc8f9bf3848b5a8fbcf2",
                        ))
                        .load::<(String, BigDecimal)>(conn);
                assert_eq!(result.unwrap(), vec![(
                    "0xeea01e0c163fe390e30afd0e6ff88a3535a2d78b4cbbcc8f9bf3848b5a8fbcf2"
                        .to_string(),
                    BigDecimal::from(49900100)
                )]);

                // we also want to check the number of rows in the table
                let result = diesel::QueryDsl::count(current_coin_balances).get_result::<i64>(conn);
                assert_eq!(result.unwrap(), 3);
                Ok(())
            })
            .await
            .is_ok());
    }
}
