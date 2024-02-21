// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// Increase recursion limit for `serde_json::json!` macro parsing
#![recursion_limit = "256"]

// #[macro_use]
// extern crate diesel_migrations;

// Need to use this for because src/schema.rs uses the macros and is autogenerated
#[macro_use]
extern crate diesel;

pub use config::IndexerGrpcProcessorConfig;

pub mod config;
pub mod db_writer;
pub mod gap_detector;
pub mod grpc_stream;
pub mod models;
pub mod processors;
pub mod schema;
pub mod utils;
pub mod worker;
