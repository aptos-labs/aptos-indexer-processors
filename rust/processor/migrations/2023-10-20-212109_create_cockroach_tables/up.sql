-- Your SQL goes here

-- transactions_cockroach
CREATE TABLE IF NOT EXISTS transactions_cockroach (
  transaction_version BIGINT UNIQUE PRIMARY KEY NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  hash VARCHAR(66) NOT NULL,
  transaction_type VARCHAR(50) NOT NULL,
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
  epoch BIGINT NOT NULL,
  parent_signature_type TEXT,
  sender VARCHAR(66),
  sequence_number BIGINT,
  max_gas_amount NUMERIC,
  expiration_timestamp_secs TIMESTAMP,
  gas_unit_price NUMERIC,
  "timestamp" TIMESTAMP,
  entry_function_id_str TEXT,
  signature jsonb,
  id VARCHAR(66),
  round BIGINT,
  previous_block_votes_bitvec jsonb,
  proposer VARCHAR(66),
  failed_proposer_indices jsonb,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX txn_cockroach_insat_index ON transactions_cockroach (inserted_at);

-- events_cockroach
CREATE TABLE events_cockroach (
  sequence_number BIGINT NOT NULL,
  creation_number BIGINT NOT NULL,
  account_address VARCHAR(66) NOT NULL,
  transaction_version BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  event_index BIGINT NOT NULL,
  event_type TEXT NOT NULL,
  data jsonb NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (
    transaction_version,
    event_index
  ),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions_cockroach (transaction_version)
);
CREATE INDEX ev_cockroach_addr_type_index ON events_cockroach (account_address);
CREATE INDEX ev_cockroach_insat_index ON events_cockroach (inserted_at);

-- wsc_cockroach
CREATE TABLE IF NOT EXISTS wsc_cockroach (
  transaction_version BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  hash VARCHAR(66) NOT NULL,
  write_set_change_type TEXT NOT NULL,
  address VARCHAR(66) NOT NULL,
  index BIGINT NOT NULL,
  payload jsonb,
  bytecode bytea,
  friends jsonb,
  exposed_functions jsonb,
  structs jsonb,
  is_deleted BOOLEAN NOT NULL,
  name TEXT,
  module TEXT,
  generic_type_params jsonb,
  state_key_hash VARCHAR(66),
  table_handle VARCHAR(66),
  decoded_key jsonb,
  decoded_value jsonb,
  key_type TEXT,
  value_type TEXT,
  key text,
  data jsonb,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, index),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions_cockroach (transaction_version)
);
CREATE INDEX wsc_cockroach_addr_type_ver_index ON wsc_cockroach (address, transaction_version DESC);
CREATE INDEX wsc_cockroach_insat_index ON wsc_cockroach (inserted_at);
