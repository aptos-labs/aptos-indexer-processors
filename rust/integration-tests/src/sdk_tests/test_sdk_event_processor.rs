// use crate::framework::test_context::TestContext;
// use crate::framework::sdk_tests::run_sdk_processor_test;
// use crate::processors::events_processor::EventsProcessorConfig;
// use diesel::{pg::PgConnection, RunQueryDsl};
// use crate::schema::events::dsl::*;
//
// #[tokio::test]
// async fn test_sdk_event_processor() {
//     let test_context = TestContext::new(&[]).await.unwrap();
//     let processor_config = EventsProcessorConfig {
//         channel_size: 10,
//         starting_version: Some(1),
//     };
//
//     run_sdk_processor_test(
//         &test_context,
//         processor_config,
//         |conn: &mut PgConnection, txn_version: &str| {
//             let events_result = events
//                 .filter(transaction_version.eq(1))
//                 .load::<(i64, i32, String)>(conn)?;
//
//             assert!(!events_result.is_empty());
//             Ok(())
//         },
//     )
//         .await
//         .unwrap();
// }
