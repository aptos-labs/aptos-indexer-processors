MERGE INTO `{}` AS main
USING (
  SELECT *
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY -- primary key(s)
            txn_version,
            write_set_change_index
          ORDER BY inserted_at DESC
        ) AS row_num
      FROM `{}`
    ) AS foo
  WHERE foo.row_num = 1
) AS staging
  ON main.txn_version = staging.txn_version -- primary key(s)
  AND main.write_set_change_index = staging.write_set_change_index
WHEN NOT MATCHED BY TARGET
THEN
  INSERT (
    txn_version,
    write_set_change_index,
    state_key_hash,
    change_type,
    resource_address,
    block_height,
    block_timestamp
  )
  VALUES (
    staging.txn_version,
    staging.write_set_change_index,
    staging.state_key_hash,
    staging.change_type,
    staging.resource_address,
    staging.block_height,
    stagin.block_timestamp
  )
;
