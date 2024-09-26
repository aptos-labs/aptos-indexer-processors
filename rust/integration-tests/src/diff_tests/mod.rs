use serde_json::Value;

mod all_tests;
pub fn remove_inserted_at(value: &mut Value) {
    if let Some(array) = value.as_array_mut() {
        for item in array.iter_mut() {
            if let Some(obj) = item.as_object_mut() {
                obj.remove("inserted_at");
            }
        }
    }
}

pub fn get_expected_imported_testnet_txns(processor_name: &str, txn_version: &str) -> String {
    format!(
        "expected_db_output_files/imported_testnet_txns/{}/{}_{}.json",
        processor_name,
        processor_name,
        txn_version
    )
}

pub fn get_expected_imported_mainnet_txns(processor_name: &str, txn_version: &str) -> String {
    format!(
        "expected_db_output_files/imported_mainnet_txns/{}/{}_{}.json",
        processor_name,
        processor_name,
        txn_version
    )
}

pub fn get_expected_generated_txns(processor_name: &str, txn_version: &str) -> String {
    format!(
        "expected_db_output_files/generated_txns/{}/{}_{}.json",
        processor_name,
        processor_name,
        txn_version
    )
}
