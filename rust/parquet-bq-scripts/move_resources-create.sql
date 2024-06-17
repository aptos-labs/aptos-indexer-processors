CREATE TABLE `{}`
(
  txn_version INT64,
  write_set_change_index INT64,
  block_height INT64,
  block_timestamp TIMESTAMP,
  resource_address STRING,
  resource_type STRING,
  module STRING,
  fun STRING,
  is_deleted BOOL,
  generic_type_params STRING,
  data STRING,
  state_key_hash STRING,
  
  bq_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY(txn_version, write_set_change_index) NOT ENFORCED
)
PARTITION BY TIMESTAMP_TRUNC(block_timestamp, DAY)
CLUSTER BY txn_version, resource_type, block_height, state_key_hash, resource_address
;
