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
    txn_version,
    write_set_change_index,
    block_height,
    block_timestamp,
    resource_address,
    resource_type,
    module,
    fun,
    is_deleted,
    generic_type_params,
    data,
    state_key_hash
  )
  VALUES (
    staging.txn_version,
    staging.write_set_change_index,
    staging.block_height,
    staging.block_timestamp,
    staging.resource_address,
    staging.resource_type,
    staging.module,
    staging.fun,
    staging.is_deleted,
    staging.generic_type_params,
    staging.data,
    staging.state_key_hash
  );
  