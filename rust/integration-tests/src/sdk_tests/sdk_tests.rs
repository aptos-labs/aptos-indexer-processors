#[cfg(test)]
mod tests {
    use crate::{
        models::queryable_models::Event, sdk::sdk_test_context,
        DiffTest, TestType,
    };
    use ahash::AHashMap;
    use aptos_indexer_test_transactions::IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_;
    use aptos_protos::transaction::v1::Transaction;
    use diesel::{pg::PgConnection, RunQueryDsl};
    use sdk_processor::{
        config::{
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::ProcessorConfig,
        },
        processors::events_processor::{EventsProcessor, EventsProcessorConfig},
        schema::events::dsl::*,
    };

    #[tokio::test]
    async fn test_run() {
        // Step 1: set up an input transaction that will be used
        let imported_txns = [IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_];
        let txn: Transaction = serde_json::from_slice(IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_).unwrap();
        
        let test_context = sdk_test_context::SdkTestContext::new(&imported_txns)
            .await
            .unwrap();
        
        // Step 2: build processor config
        let (transaction_stream_config, db_config) = test_context.create_transaction_and_db_config(Some(txn.version), Some(txn.version)).await;
        
        let events_processor_config = EventsProcessorConfig {
            channel_size: 10,
            per_table_chunk_sizes: AHashMap::new(),
        };

        let processor_config = ProcessorConfig::EventsProcessor(events_processor_config);
        
        let indexer_processor_config = IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
        };

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");
        
        // Not going to expose this. 
        let test_type = TestType::Diff(DiffTest);

        test_context
            .run(
                &events_processor,
                test_type,
                move |conn: &mut PgConnection, _txn_version: &str| {
                    // validation logic here
                    let events_result = events.load::<Event>(conn);

                    let all_events = events_result.expect("Failed to load events");
                    let json_data = serde_json::to_string_pretty(&all_events)
                        .expect("Failed to serialize events");
                    assert_eq!(
                        all_events.len(),
                        5,
                        "Expected 1 event, got {}",
                        all_events.len()
                    );
                    println!("json data: {}", json_data);
                    Ok(())
                },
            )
            .await
            .unwrap();
    }
    
}
