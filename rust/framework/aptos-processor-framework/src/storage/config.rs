// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CommonStorageConfig {
    /// If there is no minimum version in the DB, this is the version from which we'll
    /// start streaming txns.
    pub initial_starting_version: Option<u64>,

    /// Even if there is a minimum version in the DB, start streaming txns from this
    /// version.
    pub starting_version_override: Option<u64>,
}

impl CommonStorageConfig {
    pub fn determine_starting_version(&self, starting_version_from_db: Option<u64>) -> u64 {
        let (starting_version, source) = match self.starting_version_override {
            Some(version) => {
                info!("Starting from starting_version_override: {}", version);
                (version, "starting_version_override")
            },
            None => match starting_version_from_db {
                Some(version) => {
                    info!("Starting from version found in DB: {}", version);
                    (version, "starting_version_from_db")
                },
                None => match self.initial_starting_version {
                    Some(version) => {
                        info!("Starting from initial_starting_version: {}", version);
                        (version, "initial_starting_version")
                    },
                    None => {
                        info!("No starting_version_override, starting_version_from_db, or initial_starting_version. Starting from version 0");
                        (0, "default")
                    },
                },
            },
        };
        info!(
            start_version = starting_version,
            source = source,
            "Starting from version {} (source: {})",
            starting_version,
            source
        );
        starting_version
    }
}
