use crate::parquet_processors::{ParquetTypeEnum, ParquetTypeStructs};
use processor::utils::table_flags::TableFlags;
use std::collections::HashMap;

/// Fill the map with data if the table is opted in for backfill-purpose
pub fn add_to_map_if_opted_in_for_backfill(
    opt_in_tables: TableFlags,
    map: &mut HashMap<ParquetTypeEnum, ParquetTypeStructs>,
    data_types: Vec<(TableFlags, ParquetTypeEnum, ParquetTypeStructs)>,
) {
    for (table_flag, enum_type, data) in data_types {
        if opt_in_tables.is_empty() || opt_in_tables.contains(table_flag) {
            map.insert(enum_type, data);
        }
    }
}
