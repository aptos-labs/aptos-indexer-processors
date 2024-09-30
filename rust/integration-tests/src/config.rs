// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    fs,
};
use diesel::pg::PgConnection;
use processor::processors::token_v2_processor::TokenV2ProcessorConfig;
use integration_tests::{TestContext, TestProcessorConfig};
use testing_transactions::ALL_IMPORTED_TESTNET_TXNS;
use integration_tests::{
    diff_test_helper::{ProcessorTestHelper, processors::fungible_asset_processor::FungibleAssetProcessorTestHelper},
    diff_tests::{all_tests::get_processor_map, remove_inserted_at},
    TestType, DiffTest,
};
use anyhow::anyhow;

const IMPORTED_TRANSACTIONS_FOLDER: &str = "imported_transactions";

#[derive(Parser)]
pub struct IndexerCliArgs {
    /// Path to the output folder where the generated transactions will be saved.
    #[clap(long)]
    pub output_folder: PathBuf,
}

impl IndexerCliArgs {
    pub async fn run(&self) -> anyhow::Result<()> {
        println!("Output folder: {:?}", self.output_folder);
        let processor_configs = vec![
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::EventsProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::TokenV2Processor(TokenV2ProcessorConfig {
                    query_retries: 3,
                    query_retry_delay_ms: 1000,
                }),
            },
        ];

        let processor_map = get_processor_map();

        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            let test_type = TestType::Diff(DiffTest);

            if let Some(test_helper) = processor_map.get(processor_name) {
                let test_helper: Arc<Box<dyn ProcessorTestHelper>> = Arc::clone(test_helper);
                let test_context = TestContext::new(ALL_IMPORTED_MAINNET_TXNS).await.unwrap();
                let output_folder = self.output_folder.clone();

                let result = test_context
                    .run(
                        processor_config,
                        test_type,
                        move |conn: &mut PgConnection, txn_version: &str| {
                            let mut json_data = test_helper.load_data(conn, txn_version)?;
                            remove_inserted_at(&mut json_data);

                            let output_processor_folder = output_folder.join(&processor_name);
                            let output_json_file = output_processor_folder.join(format!("{}_{}.json", processor_name, txn_version));

                            if let Some(parent_dir) = output_json_file.parent() {
                                fs::create_dir_all(parent_dir)?;
                            }

                            let json_data = serde_json::to_string_pretty(&json_data)?;

                            fs::write(&output_json_file, json_data)
                                .with_context(|| format!("Failed to write file: {:?}", output_json_file))?;

                            Ok(())
                        },
                    )
                    .await;

                if result.is_err() {
                    println!("Run function encountered an error");
                }
            } else {
                return Err(anyhow!("Processor not found: {}", processor_name));
            }
        }

        Ok(())
    }
}

use super::*;
use clap::Command;
//
// #[tokio::test]
// async fn test_cli_run() {
//     // Simulate command line arguments for the CLI
//     let cli_args = vec![
//         "test_bin",                       // This would be the binary name
//         "--processor_name", "token_v2_processor", // Set processor_name argument
//         "--output_folder", "/tmp/output",        // Set the output folder argument
//     ];
//
//     let matches = IndexerCliArgs::try_parse_from(cli_args).unwrap();
//
//     let result = matches.run().await;
//
//     assert!(result.is_ok(), "CLI run should complete without errors");
//
//     let output_path = PathBuf::from("/tmp/output/token_v2_processor");
//     assert!(output_path.exists(), "Output folder for processor should exist");
// }
