CREATE TABLE `{}`
(
  key STRING,
  txn_version INT64,
  write_set_change_index INT64,
  transaction_block_height INT64,
  table_handle STRING,
  decoded_key STRING, -- json
  decoded_value STRING, -- json
  is_deleted BOOL,
  --
  bq_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY(txn_version, write_set_change_index) NOT ENFORCED
)
PARTITION BY RANGE_BUCKET(txn_version, GENERATE_ARRAY(1, 3999, 1))
;

