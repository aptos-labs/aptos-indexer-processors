
use arrow::datatypes::{DataType, Field, Schema};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref TRANSACTION_SCHEMA: Schema = Schema::new(vec![
        Field::new("version", DataType::Int64, false),
        Field::new("block_height", DataType::Int64, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("type_", DataType::Utf8, false),
        // Representing `serde_json::Value` as a string. Adjust based on your actual serialization needs.
        // Utf8 (json text), binary (serialized json), or  nested structure (expensive)
        Field::new("payload", DataType::Utf8, true),
        Field::new("state_change_hash", DataType::Utf8, false),
        Field::new("event_root_hash", DataType::Utf8, false),
        // Option<String> represented as nullable UTF8
        Field::new("state_checkpoint_hash", DataType::Utf8, true),
        // BigDecimal as String due to lack of direct support in Arrow. Consider Float64 if applicable.
        Field::new("gas_used", DataType::Utf8, false),
        Field::new("success", DataType::Boolean, false),
        Field::new("vm_status", DataType::Utf8, false),
        Field::new("accumulator_root_hash", DataType::Utf8, false),
        Field::new("num_events", DataType::Int64, false),
        Field::new("num_write_set_changes", DataType::Int64, false),
        Field::new("epoch", DataType::Int64, false),
        // Option<String> represented as nullable UTF8
        Field::new("payload_type", DataType::Utf8, true),
    ]);
}
