use crate::parquet_processors::{ParquetTypeEnum, ParquetTypeStructs};
use processor::worker::TableFlags;
use std::collections::HashMap;

/// Fill the map with data if the table is opted in for backfill-purpose
pub fn add_to_map_if_opted_in_for_backfill(
    opt_in_tables: TableFlags,
    map: &mut HashMap<ParquetTypeEnum, ParquetTypeStructs>,
    table_flag: TableFlags,
    enum_type: ParquetTypeEnum,
    data: ParquetTypeStructs,
) {
    // Add to map if all tables are opted-in (empty) or if the specific flag is set
    if opt_in_tables.is_empty() || opt_in_tables.contains(table_flag) {
        map.insert(enum_type, data);
    }
}
