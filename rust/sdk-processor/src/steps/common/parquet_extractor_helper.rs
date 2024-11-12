use crate::parquet_processors::{ParquetTypeEnum, ParquetTypeStructs};
use std::collections::HashMap;

/// Fill the map with data if the table is opted in for backfill-purpose
pub fn add_to_map_if_opted_in_for_backfill(
    opt_in_tables: &Option<Vec<String>>,
    map: &mut HashMap<ParquetTypeEnum, ParquetTypeStructs>,
    enum_type: ParquetTypeEnum,
    data: ParquetTypeStructs,
) {
    if let Some(ref backfill_table) = opt_in_tables {
        let table_name = enum_type.to_string();
        if backfill_table.contains(&table_name) {
            map.insert(enum_type, data);
        }
    } else {
        // If there's no opt-in table, include all data
        map.insert(enum_type, data);
    }
}
