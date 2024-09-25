// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// build.rs
use std::{env, fs, path::Path};

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generate_transactions.rs");

    let mut all_transactions_code = String::new();

    // Process each directory and generate code for it
    all_transactions_code.push_str(&process_directory("imported_mainnet_txns", "imported_mainnet_txns"));
    all_transactions_code.push_str(&process_directory("imported_testnet_txns", "imported_testnet_txns"));
    all_transactions_code.push_str(&process_directory("generated_txns", "generated_txns"));

    // Write the generated Rust code to the output file
    fs::write(dest_path, all_transactions_code).unwrap();
}

fn process_directory(dir_name: &str, module_name: &str) -> String {
    let mut transactions_code = String::new();
    let mut all_constants = String::new();
    let json_dir = Path::new("json_transactions").join(dir_name);

    for entry in fs::read_dir(json_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            let file_name = path.file_stem().unwrap().to_str().unwrap();
            let const_name = format!(
                "{}_{}",
                module_name.to_uppercase(),
                file_name.to_uppercase().replace('-', "_")
            );

            let json_code = format!(
                r#"
                pub const {const_name}: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/json_transactions/{dir_name}/{file_name}.json"));
                "#,
                const_name = const_name,
                dir_name = dir_name,
                file_name = file_name,
            );
            transactions_code.push_str(&json_code);
            all_constants.push_str(&format!("{},", const_name));
        }
    }

    // Add a vector containing all constants for this directory
    transactions_code.push_str(&format!(
        "pub const ALL_{}: &[&[u8]] = &[{}];",
        module_name.to_uppercase(),
        all_constants
    ));

    transactions_code
}