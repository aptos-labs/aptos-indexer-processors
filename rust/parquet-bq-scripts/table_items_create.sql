CREATE TABLE `{}`
(
  txn_version INT64,
  block_timestamp TIMESTAMP,
  write_set_change_index INT64,
  transaction_block_height INT64,
  table_key STRING,
  table_handle STRING,
  decoded_key STRING, -- json
  decoded_value STRING, -- json
  is_deleted BOOL,
  --
  bq_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY(txn_version, write_set_change_index) NOT ENFORCED
)
PARTITION BY TIMESTAMP_TRUNC(block_timestamp, DAY)
CLUSTER BY table_key, txn_version
;

