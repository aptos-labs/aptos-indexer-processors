use crate::diff_test_helper::remove_transaction_timestamp;
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
pub mod ans_processor_tests;

#[cfg(test)]
pub mod events_processor_tests;
#[cfg(test)]
pub mod fungible_asset_processor_tests;
#[cfg(test)]
pub mod token_v2_processor_tests;

#[cfg(test)]
pub mod account_transaction_processor_tests;

#[cfg(test)]
pub mod default_processor_tests;

#[cfg(test)]
pub mod objects_processor_tests;
#[cfg(test)]
pub mod stake_processor_tests;

#[cfg(test)]
pub mod account_restoration_processor_tests;
#[cfg(test)]
pub mod user_transaction_processor_tests;

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

// Common setup for database and test context
#[allow(dead_code)]
pub async fn setup_test_environment(
    transactions: &[&[u8]],
) -> (PostgresTestDatabase, SdkTestContext) {
    let mut db = PostgresTestDatabase::new();
    db.setup().await.unwrap();

    let mut test_context = SdkTestContext::new(transactions);
    if test_context.init_mock_grpc().await.is_err() {
        panic!("Failed to initialize mock grpc");
    };

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
    let txn_versions: Vec<i64> = test_context
        .get_test_transaction_versions()
        .into_iter()
        .map(|v| v as i64)
        .collect();

    let db_values = test_context
        .run(
            &processor,
            generate_file_flag,
            output_path.clone(),
            custom_file_name,
            move || {
                let mut conn = PgConnection::establish(&db_url).unwrap_or_else(|e| {
                    eprintln!("[ERROR] Failed to establish DB connection: {:?}", e);
                    panic!("Failed to establish DB connection: {:?}", e);
                });

                let db_values = match load_data(&mut conn, txn_versions.clone()) {
                    Ok(db_data) => db_data,
                    Err(e) => {
                        eprintln!("[ERROR] Failed to load data {}", e);
                        return Err(e);
                    },
                };

                if db_values.is_empty() {
                    eprintln!("[WARNING] No data found for versions: {:?}", txn_versions);
                }

                Ok(db_values)
            },
        )
        .await?;
    Ok(db_values)
}
