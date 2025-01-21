use serde::{Deserialize, Serialize};

/// This enum captures the configs for all the different db storages that are defined.
/// The configs for each db storage should only contain configuration specific to that
/// type.
#[derive(Clone, Debug, Deserialize, Serialize, strum::IntoStaticStr, strum::EnumDiscriminants)]
#[serde(tag = "type", rename_all = "snake_case")]
// What is all this strum stuff? Let me explain.
//
// Previously we had consts called NAME in each module and a function called `name` on
// the ProcessorTrait. As such it was possible for this name to not match the snake case
// representation of the struct name. By using strum we can have a single source for
// processor names derived from the enum variants themselves.
//
// That's what this strum_discriminants stuff is, it uses macro magic to generate the
// ProcessorName enum based on ProcessorConfig. The rest of the derives configure this
// generation logic, e.g. to make sure we use snake_case.
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(
    derive(
        Deserialize,
        Serialize,
        strum::EnumVariantNames,
        strum::IntoStaticStr,
        strum::Display,
        clap::ValueEnum
    ),
    name(DbTypeName),
    clap(rename_all = "snake_case"),
    serde(rename_all = "snake_case"),
    strum(serialize_all = "snake_case")
)]
pub enum DbConfig {
    PostgresConfig(PostgresConfig),
    ParquetConfig(ParquetConfig),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    pub connection_string: String,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    #[serde(default = "PostgresConfig::default_db_pool_size")]
    pub db_pool_size: u32,
}

impl PostgresConfig {
    pub const fn default_db_pool_size() -> u32 {
        150
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetConfig {
    pub connection_string: String,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    #[serde(default = "PostgresConfig::default_db_pool_size")]
    pub db_pool_size: u32,
    // Optional Google application credentials for authentication
    #[serde(default)]
    pub google_application_credentials: Option<String>,
    #[serde(default)]
    pub bucket_name: String,
    #[serde(default)]
    pub bucket_root: String,
}
