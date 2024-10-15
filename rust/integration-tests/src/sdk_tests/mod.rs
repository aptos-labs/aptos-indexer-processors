use crate::diff_tests::{remove_inserted_at, remove_transaction_timestamp};
use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
use aptos_indexer_testing_framework::{
    database::{PostgresTestDatabase, TestDatabase},
    new_test_context::SdkTestContext,
};
use assert_json_diff::assert_json_eq;
use diesel::{Connection, PgConnection};
use serde_json::Value;
use std::{collections::HashMap, fs, path::Path};

mod events_processor_tests;
mod fungible_asset_processor_tests;

const DEFAULT_OUTPUT_FOLDER: &str = "sdk_expected_db_output_files/";

pub fn read_and_parse_json(path: &str) -> anyhow::Result<Value> {
    match fs::read_to_string(path) {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(json) => Ok(json),
            Err(e) => {
                eprintln!("[ERROR] Failed to parse JSON at {}: {}", path, e);
                Err(anyhow::anyhow!("Failed to parse JSON: {}", e))
            },
        },
        Err(e) => {
            eprintln!("[ERROR] Failed to read file at {}: {}", path, e);
            Err(anyhow::anyhow!("Failed to read file: {}", e))
        },
    }
}

// Common setup for database and test context
async fn setup_test_environment(transactions: &[&[u8]]) -> (PostgresTestDatabase, SdkTestContext) {
    let mut db = PostgresTestDatabase::new();
    db.setup().await.unwrap();

    let test_context = SdkTestContext::new(transactions).await.unwrap();

    (db, test_context)
}

fn validate_json(
    db_values: &mut HashMap<String, Value>,
    txn_version: u64,
    processor_name: &str,
    output_path: String,
) -> anyhow::Result<()> {
    for (table_name, db_value) in db_values.iter_mut() {
        // Generate the expected JSON file path for each table
        let expected_file_path = Path::new(&output_path)
            .join(processor_name)
            .join(txn_version.to_string())
            .join(format!("{}.json", table_name)); // File name format: processor_table_txnVersion.json

        let mut expected_json = match read_and_parse_json(expected_file_path.to_str().unwrap()) {
            Ok(json) => json,
            Err(e) => {
                eprintln!(
                    "[ERROR] Error handling JSON for processor {} table {} and transaction version {}: {}",
                    processor_name, table_name, txn_version, e
                );
                panic!("Failed to read and parse JSON for table: {}", table_name);
            },
        };

        // TODO: Clean up non-deterministic fields (e.g., timestamps, `inserted_at`)
        remove_inserted_at(db_value);
        remove_transaction_timestamp(db_value);
        remove_inserted_at(&mut expected_json);
        remove_transaction_timestamp(&mut expected_json);

        assert_json_eq!(db_value, expected_json);
    }
    Ok(())
}

// Helper function to configure and run the processor
async fn run_processor_test<F>(
    test_context: &mut SdkTestContext,
    processor: impl ProcessorTrait,
    load_data: F,
    db_url: &str,
    txn_version: u64,
    diff_flag: bool,
    output_path: String,
) -> anyhow::Result<HashMap<String, Value>>
where
    F: Fn(&mut PgConnection, &str) -> anyhow::Result<HashMap<String, Value>>
        + Send
        + Sync
        + 'static,
{
    println!(
        "[INFO] Running {} test for transaction version: {}",
        processor.name(),
        txn_version
    );

    let db_value = test_context
        .run(
            &processor,
            db_url,
            txn_version,
            diff_flag,
            output_path.clone(),
            move |db_url| {
                // TODO: might not need this.
                let mut conn =
                    PgConnection::establish(db_url).expect("Failed to establish DB connection");

                let db_values = match load_data(&mut conn, &txn_version.to_string()) {
                    Ok(db_data) => db_data,
                    Err(e) => {
                        eprintln!(
                            "[ERROR] Failed to load data for transaction version {}: {}",
                            txn_version, e
                        );
                        return Err(e);
                    },
                };

                if db_values.is_empty() {
                    eprintln!("[WARNING] No data found for txn version: {}", txn_version);
                }

                Ok(db_values)
            },
        )
        .await?;

    Ok(db_value)
}
