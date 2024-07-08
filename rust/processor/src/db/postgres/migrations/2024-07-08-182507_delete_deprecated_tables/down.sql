-- Recreate the transactions table
CREATE TABLE transactions (
  version BIGINT UNIQUE PRIMARY KEY NOT NULL,
  block_height BIGINT NOT NULL,
  hash VARCHAR(66) UNIQUE NOT NULL,
  type VARCHAR(50) NOT NULL,
  payload jsonb,
  state_change_hash VARCHAR(66) NOT NULL,
  event_root_hash VARCHAR(66) NOT NULL,
  state_checkpoint_hash VARCHAR(66),
  gas_used NUMERIC NOT NULL,
  success BOOLEAN NOT NULL,
  vm_status TEXT NOT NULL,
  accumulator_root_hash VARCHAR(66) NOT NULL,
  num_events BIGINT NOT NULL,
  num_write_set_changes BIGINT NOT NULL,
  -- Default time columns
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

---- Recreate the move_resources table
CREATE TABLE move_resources (
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  name TEXT NOT NULL,
  address VARCHAR(66) NOT NULL,
  type TEXT NOT NULL,
  module TEXT NOT NULL,
  generic_type_params jsonb,
  data jsonb,
  is_deleted BOOLEAN NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (transaction_version, write_set_change_index),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);

-- Recreate indexes and views for move_resources
CREATE INDEX mr_addr_mod_name_ver_index ON move_resources (address, module, name, transaction_version);
CREATE INDEX mr_insat_index ON move_resources (inserted_at);
CREATE INDEX IF NOT EXISTS mr_ver_index ON move_resources(transaction_version DESC);

CREATE VIEW move_resources_view AS
SELECT transaction_version,
  write_set_change_index,
  transaction_block_height,
  name,
  address,
  "type",
  "module",
  generic_type_params,
  data#>>'{}' as json_data,
  is_deleted,
  inserted_at
FROM move_resources;

-- Recreate the address_version_from_move_resources view
CREATE OR REPLACE VIEW address_version_from_move_resources AS
SELECT address,
  transaction_version
FROM move_resources
GROUP BY 1, 2;

-- Recreate the write_set_changes table
CREATE TABLE write_set_changes (
  transaction_version BIGINT NOT NULL,
  index BIGINT NOT NULL,
  hash VARCHAR(66) NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  type TEXT NOT NULL,
  address VARCHAR(66) NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (transaction_version, index)
--  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);

-- Recreate indexes for write_set_changes
CREATE INDEX wsc_addr_type_ver_index ON write_set_changes (address, transaction_version DESC);
CREATE INDEX wsc_insat_index ON write_set_changes (inserted_at);

-- Recreate the transactions_view
CREATE VIEW transactions_view AS
SELECT "version",
  block_height,
  "hash",
  "type",
  payload#>>'{}' AS json_payload,
  state_change_hash,
  event_root_hash,
  state_checkpoint_hash,
  gas_used,
  success,
  vm_status,
  accumulator_root_hash,
  num_events,
  num_write_set_changes,
  inserted_at
FROM transactions;
