MERGE INTO `{}` AS main
USING (
  SELECT *
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY -- primary key(s)
            txn_version
        ) AS row_num
      FROM `{}`
    ) AS foo
  WHERE foo.row_num = 1
) AS staging
  ON main.txn_version = staging.txn_version -- primary key(s)
WHEN NOT MATCHED BY TARGET
THEN
  INSERT (
    txn_version,
    block_height,
    epoch,
    txn_type,
    payload,
    payload_type,
    gas_used,
    success,
    vm_status,
    num_events,
    num_write_set_changes,
    txn_hash,
    state_change_hash,
    event_root_hash,
    state_checkpoint_hash,
    accumulator_root_hash,
    block_timestamp
  )
  VALUES (
    staging.txn_version,
    staging.block_height,
    staging.epoch,
    staging.txn_type,
    staging.payload,
    staging.payload_type,
    staging.gas_used,
    staging.success,
    staging.vm_status,
    staging.num_events,
    staging.num_write_set_changes,
    staging.txn_hash,
    staging.state_change_hash,
    staging.event_root_hash,
    staging.state_checkpoint_hash,
    staging.accumulator_root_hash,
    staging.block_timestamp
  )
;
