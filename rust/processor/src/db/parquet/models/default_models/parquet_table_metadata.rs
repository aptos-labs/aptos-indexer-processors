use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::default_models::raw_table_metadata::{
        RawTableMetadata, TableMetadataConvertible,
    },
};
use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct TableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl NamedTable for TableMetadata {
    const TABLE_NAME: &'static str = "table_metadata";
}

impl HasVersion for TableMetadata {
    fn version(&self) -> i64 {
        -1
    }
}

impl GetTimeStamp for TableMetadata {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[allow(deprecated)]
        chrono::NaiveDateTime::from_timestamp(0, 0)
    }
}

impl TableMetadataConvertible for TableMetadata {
    fn from_raw(raw_item: &RawTableMetadata) -> Self {
        Self {
            handle: raw_item.handle.clone(),
            key_type: raw_item.key_type.clone(),
            value_type: raw_item.value_type.clone(),
        }
    }
}
