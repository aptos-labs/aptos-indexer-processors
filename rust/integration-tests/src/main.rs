// main.rs
use anyhow::{anyhow, Result};
use diesel::{pg::PgConnection, query_dsl::methods::FilterDsl, ExpressionMethods, RunQueryDsl};
use integration_tests::{TestContext, TestProcessorConfig, TestType, DiffTest};
// use processor::schema::events::dsl::*;
use std::fs;
use testing_transactions::{
     ALL_IMPORTED_TESTNET_TXNS,
};
mod models;
use crate::models::queryable_models::{ FungibleAssetActivity};
use processor::schema::fungible_asset_activities::dsl::{fungible_asset_activities, transaction_version as transaction_version_fa};


// TODO: Support cli args for specifying the processor to run.
#[allow(clippy::needless_return)]
#[tokio::main]
async fn main() -> Result<()> {


    let test_context = TestContext::new(ALL_IMPORTED_TESTNET_TXNS).await.unwrap();

    let processor_config = TestProcessorConfig {
        config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
    };

    let processor_name = processor::processors::ProcessorConfig::FungibleAssetProcessor.name();
    let test_type = TestType::Diff(DiffTest);

    let result = test_context
        .run(
            processor_config,
            test_type,
            move |conn: &mut PgConnection, txn_version: &str| {
                // let events_result = events
                //     .filter(transaction_version.eq(txn_version.parse::<i64>().unwrap()))
                //     .load::<Event>(conn);

                let fungible_asset_activities_result = fungible_asset_activities
                    .filter(transaction_version_fa.eq(txn_version.parse::<i64>().unwrap()))
                    .load::<FungibleAssetActivity>(conn);

                let results = match fungible_asset_activities_result {
                    Ok(results) => results,
                    Err(e) => return Err(anyhow!("Error loading deposit events: {}", e)),
                };

                // Serialize to pretty JSON
                let json_data = match serde_json::to_string_pretty(&results) {
                    Ok(json) => json,
                    Err(e) => return Err(anyhow!("Error serializing events: {}", e)),
                };

                // Write serialized canonical JSON to file
                // if let Err(e) = fs::write(&output_json_file, &json_data) {
                //     return Err(anyhow!("Failed to write JSON to file: {}", e));
                // }
                // Alternatively, use a custom serializer for canonical JSON
                // Write the canonical-serialized pretty JSON to another file (if needed)
                // let mut output_json_file = File::create("db_output_canonical_pretty.json")?;
                // output_json_file.write_all(json.as_bytes())?;
                // Write JSON data to file
                let output_json_file = format!("{}_{}.json", processor_name, txn_version);
                // println!("Writing JSON data to file: {}", json_data);
                println!("Attempting to write file: {}", output_json_file);

                match fs::write(&output_json_file, &json_data) {
                    Ok(_) => println!("File written successfully: {}", output_json_file),
                    Err(e) => {
                        println!("Failed to write file: {}", e);
                        return Err(anyhow!("Error writing file: {}", e));
                    },
                }

                Ok(())
            },
        )
        .await;
    if result.is_err() {
        println!("Run function encountered an error");
    }

    Ok(())
}
