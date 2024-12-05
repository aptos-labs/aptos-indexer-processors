use aptos_protos::transaction::v1::WriteTableItem;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawTableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl RawTableMetadata {
    pub fn from_write_table_item(table_item: &WriteTableItem) -> Self {
        Self {
            handle: table_item.handle.to_string(),
            key_type: table_item.data.as_ref().unwrap().key_type.clone(),
            value_type: table_item.data.as_ref().unwrap().value_type.clone(),
        }
    }
}

pub trait TableMetadataConvertible {
    fn from_raw(raw_item: &RawTableMetadata) -> Self;
}
