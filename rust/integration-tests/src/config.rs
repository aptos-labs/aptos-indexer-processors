// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Context};
use clap::Parser;
use diesel::pg::PgConnection;
use integration_tests::{
    diff_test_helper::ProcessorTestHelper,
    diff_tests::{all_tests::get_processor_map, remove_inserted_at, remove_transaction_timestamp},
    DiffTest, TestType,
};
use processor::processors::token_v2_processor::TokenV2ProcessorConfig;
use std::{fs, path::PathBuf, sync::Arc};
// use aptos_indexer_test_transactions::{
//     ALL_SCRIPTED_TRANSACTIONS, ALL_IMPORTED_MAINNET_TXNS, ALL_IMPORTED_TESTNET_TXNS,
// };
use testing_transactions::{
    ALL_IMPORTED_MAINNET_TXNS, ALL_IMPORTED_TESTNET_TXNS, ALL_SCRIPTED_TRANSACTIONS,
};

const IMPORTED_TESTNET_FOLDER: &str = "expected_db_output_files/imported_testnet_txns";
const IMPORTED_MAINNET_FOLDER: &str = "expected_db_output_files/imported_mainnet_txns";
const SCRIPTED_FOLDER: &str = "expected_db_output_files/scripted_txns";
// Constants for test context types
const IMPORTED_TESTNET: &str = "imported_testnet";
const IMPORTED_MAINNET: &str = "imported_mainnet";
const SCRIPTED: &str = "scripted";

#[derive(Parser)]
#[clap(name = "Indexer db expected schema output CLI")]
pub struct IndexerCliArgs {
    /// List of test contexts to create, e.g., imported_testnet, mainnet, scripted
    #[clap(long, num_args = 1.., required = true)]
    pub test_contexts: Vec<String>,
}

impl IndexerCliArgs {
    pub async fn run(&self) -> anyhow::Result<()> {
        let processor_configs = vec![
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::EventsProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::TokenV2Processor(
                    TokenV2ProcessorConfig {
                        query_retries: 3,
                        query_retry_delay_ms: 1000,
                    },
                ),
            },
        ];

        let processor_map = get_processor_map();

        // Loop over the specified test contexts
        for test_context_name in self.test_contexts.clone() {
            // Define the root folder based on the test context name

            let root_folder = match test_context_name.as_str() {
                IMPORTED_TESTNET => PathBuf::from(IMPORTED_TESTNET_FOLDER),
                IMPORTED_MAINNET => PathBuf::from(IMPORTED_MAINNET_FOLDER),
                SCRIPTED => PathBuf::from(SCRIPTED_FOLDER),
                _ => {
                    return Err(anyhow!(
                        "No TestContext found for type: {}",
                        test_context_name
                    ))
                },
            };

            // Ensure the root folder exists
            fs::create_dir_all(&root_folder)
                .with_context(|| format!("Failed to create root folder: {:?}", root_folder))?;
            println!("Root folder: {:?}", root_folder);

            // Match the test context name to generate the appropriate TestContext
            let test_context = match test_context_name.as_str() {
                IMPORTED_TESTNET => TestContext::new(ALL_IMPORTED_TESTNET_TXNS).await,
                IMPORTED_MAINNET => TestContext::new(ALL_IMPORTED_MAINNET_TXNS).await,
                SCRIPTED => TestContext::new(ALL_SCRIPTED_TRANSACTIONS).await,
                _ => {
                    return Err(anyhow!(
                        "No TestContext found for type: {}",
                        test_context_name
                    ))
                },
            }?;

            for processor_config in &processor_configs {
                let processor_name = processor_config.config.name();
                let test_type = TestType::Diff(DiffTest);
                if let Some(test_helper) = processor_map.get(processor_name) {
                    let test_helper: Arc<Box<dyn ProcessorTestHelper>> = Arc::clone(test_helper);

                    // Clone the root folder so it can be used inside the closure
                    let output_folder = root_folder.clone();

                    // Precompute the transaction names before the closure
                    // let txn_names_map: HashMap<String, String> = test_context
                    //     .transaction_batches
                    //     .iter()
                    //     .map(|txn| {
                    //         let version = txn.version.to_string();
                    //         let name = test_context
                    //             .(&version)
                    //             .unwrap_or_else(|| "unknown".to_string());
                    //         (version, name)
                    //     })
                    //     .collect(); // Collect into a HashMap of version -> name
                    // test_context.transaction_batches.iter().map(|txn| {
                    //
                    //     // let version = txn.version.to_string();
                    //     // let name = test_context
                    //     //     .transaction_names
                    //     //     .get(&version)
                    //     //     .unwrap_or_else(|| "unknown".to_string());
                    //     // (version, name)
                    // }).collect::<HashMap<String, String>>(

                    let cloned_test_context_name = test_context_name.clone();
                    let result = test_context
                        .run(processor_config.clone(), test_type, {
                            move |conn: &mut PgConnection, txn_version: &str, txn_name: &str| {
                                let mut json_data = test_helper.load_data(conn, txn_version)?;
                                remove_inserted_at(&mut json_data);
                                remove_transaction_timestamp(&mut json_data);

                                let output_processor_folder = output_folder.join(processor_name);

                                let output_json_file = if cloned_test_context_name == SCRIPTED {
                                    output_processor_folder
                                        .join(format!("{}_{}.json", processor_name, txn_name))
                                } else {
                                    output_processor_folder
                                        .join(format!("{}_{}.json", processor_name, txn_version))
                                };

                                if let Some(parent_dir) = output_json_file.parent() {
                                    fs::create_dir_all(parent_dir)?;
                                }

                                let json_data = serde_json::to_string_pretty(&json_data)?;

                                fs::write(&output_json_file, json_data).with_context(|| {
                                    format!("Failed to write file: {:?}", output_json_file)
                                })?;
                                println!("Created a file: {:?}", output_json_file);
                                Ok(())
                            }
                        })
                        .await;

                    if result.is_err() {
                        println!(
                            "Run function encountered an error for processor {}",
                            processor_name
                        );
                    }
                } else {
                    return Err(anyhow!("Processor not found: {}", processor_name));
                }
            }
        }

        Ok(())
    }
}

use super::*;

#[allow(clippy::needless_return)]
#[tokio::test]
async fn test_cli_run() {
    // Simulate command line arguments for the CLI
    let cli_args = vec![
        "test_bin", // This would be the binary name
        "--test-contexts",
        "scripted", // Set test_contexts argument
    ];

    let matches = IndexerCliArgs::try_parse_from(cli_args).unwrap();

    let result = matches.run().await;

    assert!(result.is_ok(), "CLI run should complete without errors");

    let output_path = PathBuf::from("expected_db_output_files/scripted_txns/token_v2_processor");
    assert!(
        output_path.exists(),
        "Output folder for processor should exist"
    );
}
