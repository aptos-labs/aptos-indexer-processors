use crate::{
    parquet_processors::ParquetTypeEnum,
    processors::{
        ans_processor::AnsProcessorConfig, nft_metadata_processor::NftMetadataProcessorConfig,
        objects_processor::ObjectsProcessorConfig, stake_processor::StakeProcessorConfig,
        token_v2_processor::TokenV2ProcessorConfig,
    },
};
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use strum::IntoEnumIterator;

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
    NftMetadataProcessor(NftMetadataProcessorConfig),
    UserTransactionProcessor(DefaultProcessorConfig),
    StakeProcessor(StakeProcessorConfig),
    TokenV2Processor(TokenV2ProcessorConfig),
    ObjectsProcessor(ObjectsProcessorConfig),
    MonitoringProcessor(DefaultProcessorConfig),
    // ParquetProcessor
    ParquetDefaultProcessor(ParquetDefaultProcessorConfig),
}

impl ProcessorConfig {
    /// Get the name of the processor config as a static str. This is a convenience
    /// method to access the derived functionality implemented by strum::IntoStaticStr.
    pub fn name(&self) -> &'static str {
        self.into()
    }

    /// Get the Vec of table names for parquet processors only.
    ///
    /// This is a convenience method to map the table names to include the processor name as a prefix, which
    /// is useful for querying the status from the processor status table in the database.
    pub fn get_table_names(&self) -> anyhow::Result<Vec<String>> {
        match self {
            ProcessorConfig::ParquetDefaultProcessor(config) => {
                // Get the processor name as a prefix
                let prefix = self.name();

                // Collect valid table names from `ParquetTypeEnum` into a set for quick lookup
                let valid_table_names: HashSet<String> =
                    ParquetTypeEnum::iter().map(|e| e.to_string()).collect();

                // Validate and map table names with prefix
                let mut validated_table_names = Vec::new();
                for table_name in &config.tables {
                    // Ensure the table name is a valid `ParquetTypeEnum` variant
                    if !valid_table_names.contains(table_name) {
                        return Err(anyhow::anyhow!(
                            "Invalid table name '{}'. Expected one of: {:?}",
                            table_name,
                            valid_table_names
                        ));
                    }

                    // Append the prefix to the validated table name
                    validated_table_names.push(Self::format_table_name(prefix, table_name));
                }

                Ok(validated_table_names)
            },
            _ => Err(anyhow::anyhow!(
                "Invalid parquet processor config: {:?}",
                self
            )),
        }
    }

    /// helper function to format the table name with the processor name.
    fn format_table_name(prefix: &str, table_name: &str) -> String {
        format!("{}.{}", prefix, table_name)
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
    #[serde(default = "ParquetDefaultProcessorConfig::default_channel_size")]
    pub channel_size: usize,
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
    pub const fn default_channel_size() -> usize {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_table_names() {
        let config = ProcessorConfig::ParquetDefaultProcessor(ParquetDefaultProcessorConfig {
            tables: HashSet::from(["MoveResource".to_string(), "Transaction".to_string()]),
            bucket_name: "bucket_name".to_string(),
            bucket_root: "bucket_root".to_string(),
            google_application_credentials: None,
            channel_size: 10,
            max_buffer_size: 100000,
            parquet_upload_interval: 1800,
        });

        let result = config.get_table_names();
        assert!(result.is_ok());

        let table_names = result.unwrap();
        let table_names: HashSet<String> = table_names.into_iter().collect();
        let expected_names: HashSet<String> =
            ["Transaction".to_string(), "MoveResource".to_string()]
                .iter()
                .map(|e| format!("parquet_default_processor.{}", e))
                .collect();
        assert_eq!(table_names, expected_names);
    }

    #[test]
    fn test_invalid_table_name() {
        let config = ProcessorConfig::ParquetDefaultProcessor(ParquetDefaultProcessorConfig {
            tables: HashSet::from(["InvalidTable".to_string(), "Transaction".to_string()]),
            bucket_name: "bucket_name".to_string(),
            bucket_root: "bucket_root".to_string(),
            google_application_credentials: None,
            channel_size: 10,
            max_buffer_size: 100000,
            parquet_upload_interval: 1800,
        });

        let result = config.get_table_names();
        assert!(result.is_err());

        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("Invalid table name 'InvalidTable'"));
        assert!(error_message.contains("Expected one of:"));
    }

    #[test]
    fn test_empty_tables() {
        let config = ProcessorConfig::ParquetDefaultProcessor(ParquetDefaultProcessorConfig {
            tables: HashSet::new(),
            bucket_name: "bucket_name".to_string(),
            bucket_root: "bucket_root".to_string(),
            google_application_credentials: None,
            channel_size: 10,
            max_buffer_size: 100000,
            parquet_upload_interval: 1800,
        });
        let result = config.get_table_names();
        assert!(result.is_ok());

        let table_names = result.unwrap();
        assert_eq!(table_names, Vec::<String>::new());
    }

    #[test]
    fn test_duplicate_table_names() {
        let config = ProcessorConfig::ParquetDefaultProcessor(ParquetDefaultProcessorConfig {
            tables: HashSet::from(["Transaction".to_string(), "Transaction".to_string()]),
            bucket_name: "bucket_name".to_string(),
            bucket_root: "bucket_root".to_string(),
            google_application_credentials: None,
            channel_size: 10,
            max_buffer_size: 100000,
            parquet_upload_interval: 1800,
        });

        let result = config.get_table_names();
        assert!(result.is_ok());

        let table_names = result.unwrap();
        assert_eq!(
            table_names,
            vec!["parquet_default_processor.Transaction".to_string(),]
        );
    }

    #[test]
    fn test_all_enum_table_names() {
        let config = ProcessorConfig::ParquetDefaultProcessor(ParquetDefaultProcessorConfig {
            tables: ParquetTypeEnum::iter().map(|e| e.to_string()).collect(),
            bucket_name: "bucket_name".to_string(),
            bucket_root: "bucket_root".to_string(),
            google_application_credentials: None,
            channel_size: 10,
            max_buffer_size: 100000,
            parquet_upload_interval: 1800,
        });

        let result = config.get_table_names();
        assert!(result.is_ok());

        let table_names = result.unwrap();
        let expected_names: HashSet<String> = ParquetTypeEnum::iter()
            .map(|e| format!("parquet_default_processor.{}", e))
            .collect();
        let table_names: HashSet<String> = table_names.into_iter().collect();
        assert_eq!(table_names, expected_names);
    }
}
