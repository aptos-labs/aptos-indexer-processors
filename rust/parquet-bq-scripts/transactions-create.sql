CREATE TABLE `{}`
(
  txn_version INT64,
  block_height INT64,
  epoch INT64,
  txn_type STRING,
  payload STRING, 
  payload_type STRING,
  gas_used INT64,
  success BOOL,
  vm_status STRING,
  num_events INT64,
  num_write_set_changes INT64,
  txn_hash STRING,
  state_change_hash STRING,
  event_root_hash STRING,
  state_checkpoint_hash STRING,
  accumulator_root_hash STRING,
  block_timestamp TIMESTAMP,
  --  
  bq_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY(txn_version) NOT ENFORCED
)
PARTITION BY TIMESTAMP_TRUNC(block_timestamp,DAY)
CLUSTER BY txn_version, txn_type, block_height, txn_hash
;
