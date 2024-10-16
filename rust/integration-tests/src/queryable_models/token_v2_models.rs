use bigdecimal::BigDecimal;
use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::token_activities_v2;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = token_activities_v2)]
pub struct TokenActivityV2 {
    pub transaction_version: i64,
    pub event_index: i64,
    pub event_account_address: String,
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub type_: String,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: BigDecimal,
    pub before_value: Option<String>,
    pub after_value: Option<String>,
    pub entry_function_id_str: Option<String>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub inserted_at: chrono::NaiveDateTime,
}
