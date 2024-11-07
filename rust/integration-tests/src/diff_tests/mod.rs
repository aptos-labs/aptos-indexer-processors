// mod all_tests;

use serde_json::Value;

#[allow(dead_code)]
pub fn remove_inserted_at(value: &mut Value) {
    if let Some(array) = value.as_array_mut() {
        for item in array.iter_mut() {
            if let Some(obj) = item.as_object_mut() {
                obj.remove("inserted_at");
            }
        }
    }
}

#[allow(dead_code)]
pub fn remove_transaction_timestamp(value: &mut Value) {
    if let Some(array) = value.as_array_mut() {
        for item in array.iter_mut() {
            if let Some(obj) = item.as_object_mut() {
                obj.remove("transaction_timestamp");
            }
        }
    }
}

#[allow(dead_code)]
pub fn get_expected_imported_testnet_txns(
    processor_name: &str,
    txn_version: &str,
    table_name: &str,
) -> String {
    format!(
        "expected_db_output_files/imported_testnet_txns/{}/{}/{}.json",
        processor_name, txn_version, table_name
    )
}

#[allow(dead_code)]
pub fn get_expected_imported_mainnet_txns(
    processor_name: &str,
    txn_version: &str,
    table_name: &str,
) -> String {
    format!(
        "expected_db_output_files/imported_mainnet_txns/{}/{}/{}.json",
        processor_name, txn_version, table_name
    )
}

#[allow(dead_code)]
pub fn get_expected_scripted_txns(
    processor_name: &str,
    txn_version: &str,
    table_name: &str,
) -> String {
    format!(
        "expected_db_output_files/scripted_txns/{}/{}/{}.json",
        processor_name, txn_version, table_name
    )
}
