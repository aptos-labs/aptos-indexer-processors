use crate::config::processor_config::{ProcessorConfig, ProcessorName};
use lazy_static::lazy_static;
use std::collections::{HashMap, HashSet};
use strum::IntoEnumIterator;

lazy_static! {
    pub static ref VALID_TABLE_NAMES: HashMap<String, HashSet<String>> = {
        let mut map = HashMap::new();
        for processor_name in ProcessorName::iter() {
            map.insert(
                processor_name.to_string(),
                ProcessorConfig::table_names(&processor_name),
            );
        }
        map
    };
}

/// helper function to format the table name with the processor name.
pub fn format_table_name(prefix: &str, table_name: &str) -> String {
    format!("{}.{}", prefix, table_name)
}
