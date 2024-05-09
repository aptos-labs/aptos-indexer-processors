
#[derive(
    Associations, Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize,
)]
#[diesel(belongs_to(Transaction, foreign_key = transaction_version))]
#[diesel(primary_key(transaction_version, index))]
#[diesel(table_name = write_set_changes)]
pub struct WriteSetChange {
    pub transaction_version: i64,
    pub index: i64,
    pub hash: String,
    transaction_block_height: i64,
    pub type_: String,
    pub address: String,
}