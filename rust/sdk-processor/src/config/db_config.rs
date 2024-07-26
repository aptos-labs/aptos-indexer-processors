use ahash::AHashMap;
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
    PostgresDbConfig(PostgresDbConfig),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresDbConfig {
    pub connection_string: String,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    #[serde(default = "PostgresDbConfig::default_db_pool_size")]
    pub db_pool_size: u32,
    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
}

impl PostgresDbConfig {
    pub const fn default_db_pool_size() -> u32 {
        150
    }
}

#[derive(Debug)]
// To ensure that the variants of ProcessorConfig and Processor line up, in the testing
// build path we derive EnumDiscriminants on this enum as well and make sure the two
// sets of variants match up in `test_processor_names_complete`.
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(
        derive(strum::EnumVariantNames),
        name(DbTypeDiscriminants),
        strum(serialize_all = "snake_case")
    )
)]
pub enum DbType {
    PostgresDbType,
}

#[cfg(test)]
mod test {
    use super::*;
    use strum::VariantNames;

    /// This test exists to make sure that when a new processor is added, it is added
    /// to both Processor and ProcessorConfig. To make sure this passes, make sure the
    /// variants are in the same order (lexicographical) and the names match.
    #[test]
    fn test_processor_names_complete() {
        assert_eq!(DbTypeName::VARIANTS, DbTypeDiscriminants::VARIANTS);
    }
}
