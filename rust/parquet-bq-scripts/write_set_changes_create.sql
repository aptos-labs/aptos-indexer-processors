CREATE TABLE `{}`
(
  txn_version INT64,
  write_set_change_index INT64,
  state_key_hash STRING,
  change_type STRING,
  resource_address STRING,
  block_height INT64,
  block_timestamp TIMESTAMP,
  --
  bq_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY(txn_version, write_set_change_index) NOT ENFORCED
)
PARTITION BY TIMESTAMP_TRUNC(block_timestamp, DAY)
CLUSTER BY txn_version, change_type, block_height, state_key_hash
;