use lazy_static::lazy_static;
use std::collections::{HashMap, HashSet};

lazy_static! {
    pub static ref VALID_TABLE_NAMES: HashMap<&'static str, HashSet<String>> = {
        let mut map = HashMap::new();
        map.insert(
            "parquet_default_processor",
            HashSet::from([
                "move_resources".to_string(),
                "transactions".to_string(),
                "write_set_changes".to_string(),
                "table_items".to_string(),
                "move_modules".to_string(),
            ]),
        );
        map
    };
}
