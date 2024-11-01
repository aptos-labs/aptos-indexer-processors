use crate::diff_tests::remove_transaction_timestamp;
use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
use aptos_indexer_testing_framework::{
    database::{PostgresTestDatabase, TestDatabase},
    sdk_test_context::{remove_inserted_at, SdkTestContext},
};
use assert_json_diff::assert_json_eq;
use diesel::{Connection, PgConnection};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

#[cfg(test)]
pub mod events_processor_tests;
#[cfg(test)]
pub mod fungible_asset_processor_tests;
#[cfg(test)]
pub mod token_v2_processor_tests;

#[cfg(test)]
pub mod default_processor_tests;

#[allow(dead_code)]
pub const DEFAULT_OUTPUT_FOLDER: &str = "sdk_expected_db_output_files";

#[allow(dead_code)]
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

#[allow(dead_code)]
pub fn get_transaction_version_from_test_context(test_context: &SdkTestContext) -> Vec<u64> {
    test_context
        .transaction_batches
        .iter()
        .map(|txn| txn.version)
        .collect()
}

// Common setup for database and test context
#[allow(dead_code)]
pub async fn setup_test_environment(
    transactions: &[&[u8]],
) -> (PostgresTestDatabase, SdkTestContext) {
    let mut db = PostgresTestDatabase::new();
    db.setup().await.unwrap();

    let test_context = SdkTestContext::new(transactions).await.unwrap();

    (db, test_context)
}

#[allow(dead_code)]
pub fn validate_json(
    db_values: &mut HashMap<String, Value>,
    txn_version: u64,
    processor_name: &str,
    output_path: String,
    file_name: Option<String>,
) -> anyhow::Result<()> {
    for (table_name, db_value) in db_values.iter_mut() {
        let expected_file_path = match file_name.clone() {
            Some(custom_name) => {
                PathBuf::from(&output_path)
                    .join(processor_name)
                    .join(custom_name.clone())
                    .join(
                        format!("{}.json", table_name), // Including table_name in the format
                    )
            },
            None => {
                // Default case: use table_name and txn_version to construct file name
                Path::new(&output_path)
                    .join(processor_name)
                    .join(txn_version.to_string())
                    .join(format!("{}.json", table_name)) // File name format: processor_table_txnVersion.json
            },
        };

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
        println!(
            "Diffing table: {}, diffing version: {}",
            table_name, txn_version
        );
        assert_json_eq!(db_value, expected_json);
    }
    Ok(())
}

// Helper function to configure and run the processor
#[allow(dead_code)]
pub async fn run_processor_test<F>(
    test_context: &mut SdkTestContext,
    processor: impl ProcessorTrait,
    load_data: F,
    db_url: String,
    txn_versions: Vec<i64>,
    generate_file_flag: bool,
    output_path: String,
    custom_file_name: Option<String>,
) -> anyhow::Result<HashMap<String, Value>>
where
    F: Fn(&mut PgConnection, Vec<i64>) -> anyhow::Result<HashMap<String, Value>>
        + Send
        + Sync
        + 'static,
{
    let db_values = test_context
        .run(
            &processor,
            txn_versions[0] as u64,
            generate_file_flag,
            output_path.clone(),
            custom_file_name,
            move || {
                let mut conn =
                    PgConnection::establish(&db_url).expect("Failed to establish DB connection");

                let starting_version = txn_versions[0];
                let ending_version = txn_versions[txn_versions.len() - 1];

                let db_values = match load_data(&mut conn, txn_versions) {
                    Ok(db_data) => db_data,
                    Err(e) => {
                        eprintln!(
                            "[ERROR] Failed to load data {}", e
                        );
                        return Err(e);
                    },
                };

                if db_values.is_empty() {
                    eprintln!("[WARNING] No data found for starting txn version: {} and ending txn version {}", starting_version, ending_version);
                }

                Ok(db_values)
            },
        )
        .await?;
    Ok(db_values)
}
