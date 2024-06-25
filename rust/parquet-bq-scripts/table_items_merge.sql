MERGE INTO `{}` AS main
USING (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY -- primary key(s)
          txn_version,
          write_set_change_index
      ) AS row_num
    FROM `{}`
  ) AS foo
  WHERE foo.row_num = 1
) AS staging
  ON
    main.txn_version = staging.txn_version -- primary key(s)
    AND main.write_set_change_index = staging.write_set_change_index
WHEN NOT MATCHED BY TARGET
THEN
  INSERT (
    key,
    txn_version,
    write_set_change_index,
    transaction_block_height,
    table_handle,
    decoded_key,
    decoded_value,
    is_deleted,
    block_timestamp,
  )
  VALUES (
    staging.key,
    staging.txn_version,
    staging.write_set_change_index,
    staging.transaction_block_height,
    staging.table_handle,
    staging.decoded_key,
    staging.decoded_value,
    staging.is_deleted,
    staging.block_timestamp,

    CAST(FLOOR(staging.transaction_block_height / 1e6) AS INT64),
  );