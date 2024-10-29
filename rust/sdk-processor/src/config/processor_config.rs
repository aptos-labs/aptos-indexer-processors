use crate::processors::{
    ans_processor::AnsProcessorConfig, stake_processor::StakeProcessorConfig,
    token_v2_processor::TokenV2ProcessorConfig,
};
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
/// This enum captures the configs for all the different processors that are defined.
///
/// The configs for each processor should only contain configuration specific to that
/// processor. For configuration that is common to all processors, put it in
/// IndexerGrpcProcessorConfig.
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
    name(ProcessorName),
    clap(rename_all = "snake_case"),
    serde(rename_all = "snake_case"),
    strum(serialize_all = "snake_case")
)]
pub enum ProcessorConfig {
    AccountTransactionsProcessor(DefaultProcessorConfig),
    AnsProcessor(AnsProcessorConfig),
    DefaultProcessor(DefaultProcessorConfig),
    EventsProcessor(DefaultProcessorConfig),
    FungibleAssetProcessor(DefaultProcessorConfig),
    StakeProcessor(StakeProcessorConfig),
    TokenV2Processor(TokenV2ProcessorConfig),
    // ParquetProcessor
    ParquetDefaultProcessor(ParquetDefaultProcessorConfig),
}

impl ProcessorConfig {
    /// Get the name of the processor config as a static str. This is a convenience
    /// method to access the derived functionality implemented by strum::IntoStaticStr.
    pub fn name(&self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultProcessorConfig {
    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    // Size of channel between steps
    #[serde(default = "DefaultProcessorConfig::default_channel_size")]
    pub channel_size: usize,
    // String vector for deprecated tables to skip db writes
    #[serde(default)]
    pub deprecated_tables: HashSet<String>,
}

impl DefaultProcessorConfig {
    pub const fn default_channel_size() -> usize {
        10
    }
}

impl Default for DefaultProcessorConfig {
    fn default() -> Self {
        Self {
            per_table_chunk_sizes: AHashMap::new(),
            channel_size: Self::default_channel_size(),
            deprecated_tables: HashSet::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetDefaultProcessorConfig {
    // Optional Google application credentials for authentication
    #[serde(default)]
    pub google_application_credentials: Option<String>,
    #[serde(default)]
    pub bucket_name: String,
    #[serde(default)]
    pub bucket_root: String,
    #[serde(
        default = "ParquetDefaultProcessorConfig::default_parquet_handler_response_channel_size"
    )]
    pub parquet_handler_response_channel_size: usize,
    #[serde(default = "ParquetDefaultProcessorConfig::default_max_buffer_size")]
    pub max_buffer_size: usize,
    #[serde(default = "ParquetDefaultProcessorConfig::default_parquet_upload_interval")]
    pub parquet_upload_interval: u64,
    // list of table names to backfill. Using HashSet for fast lookups, and for future extensibility.
    #[serde(default)]
    pub tables: HashSet<String>,
}

impl ParquetDefaultProcessorConfig {
    /// Make the default very large on purpose so that by default it's not chunked
    /// This prevents any unexpected changes in behavior
    pub const fn default_parquet_handler_response_channel_size() -> usize {
        100_000
    }

    /// Default maximum buffer size for parquet files in bytes
    pub const fn default_max_buffer_size() -> usize {
        1024 * 1024 * 100 // 100 MB
    }

    /// Default upload interval for parquet files in seconds
    pub const fn default_parquet_upload_interval() -> u64 {
        1800 // 30 minutes
    }
}
