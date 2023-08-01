CREATE TABLE transactions (
  version BIGINT  PRIMARY KEY NOT NULL,
  block_height BIGINT NOT NULL,
  hash VARCHAR(66)  NOT NULL,
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
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX txn_insat_index ON transactions (inserted_at);
CREATE UNIQUE INDEX tx_v_index ON transactions (version);
CREATE UNIQUE INDEX tx_h_index ON transactions (hash);

CREATE TABLE block_metadata_transactions (
  version BIGINT  PRIMARY KEY NOT NULL,
  block_height BIGINT  NOT NULL,
  id VARCHAR(66) NOT NULL,
  round BIGINT NOT NULL,
  epoch BIGINT NOT NULL,
  previous_block_votes_bitvec jsonb NOT NULL,
  proposer VARCHAR(66) NOT NULL,
  failed_proposer_indices jsonb NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  -- Default time columns
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Constraints
  CONSTRAINT bmt_fk_versions FOREIGN KEY (version) REFERENCES transactions (version)
);
CREATE INDEX bmt_insat_index ON block_metadata_transactions (inserted_at);
CREATE UNIQUE INDEX bmt_v_index ON block_metadata_transactions (version);
CREATE UNIQUE INDEX bmt_h_index ON block_metadata_transactions (block_height);

CREATE TABLE user_transactions (
  version BIGINT PRIMARY KEY NOT NULL,
  block_height BIGINT NOT NULL,
  parent_signature_type VARCHAR(50) NOT NULL,
  sender VARCHAR(66) NOT NULL,
  sequence_number BIGINT NOT NULL,
  max_gas_amount NUMERIC NOT NULL,
  expiration_timestamp_secs TIMESTAMPTZ NOT NULL,
  gas_unit_price NUMERIC NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  entry_function_id_str text NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ut_fk_versions FOREIGN KEY (version) REFERENCES transactions (version)
);
CREATE INDEX ut_sender_seq_index ON user_transactions (sender, sequence_number);
CREATE INDEX ut_insat_index ON user_transactions (inserted_at);
CREATE UNIQUE INDEX ut_sender_seq_index ON user_transactions (sender, sequence_number);
CREATE UNIQUE INDEX ut_v_index ON user_transactions (version);

CREATE TABLE signatures (
  transaction_version BIGINT NOT NULL,
  multi_agent_index BIGINT NOT NULL,
  multi_sig_index BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  signer VARCHAR(66) NOT NULL,
  is_sender_primary BOOLEAN NOT NULL,
  type VARCHAR(50) NOT NULL,
  public_key VARCHAR(66) NOT NULL,
  signature VARCHAR(200) NOT NULL,
  threshold BIGINT NOT NULL,
  public_key_indices jsonb NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (
    transaction_version,
    multi_agent_index,
    multi_sig_index,
    is_sender_primary
  ),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);
CREATE INDEX sig_insat_index ON signatures (inserted_at);
/** Ex:
 {
 "key": "0x0400000000000000000000000000000000000000000000000000000000000000000000000a550c18",
 "sequence_number": "0",
 "type": "0x1::reconfiguration::NewEpochEvent",
 "data": {
 "epoch": "1"
 }
 }
 */
CREATE TABLE events (
  sequence_number BIGINT NOT NULL,
  creation_number BIGINT NOT NULL,
  account_address VARCHAR(66) NOT NULL,
  transaction_version BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  type TEXT NOT NULL,
  data jsonb NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (
    account_address,
    creation_number,
    sequence_number
  ),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);
CREATE INDEX ev_addr_type_index ON events (account_address);
CREATE INDEX ev_insat_index ON events (inserted_at);
-- write set changes
CREATE TABLE write_set_changes (
  transaction_version BIGINT NOT NULL,
  index BIGINT NOT NULL,
  hash VARCHAR(66) NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  type TEXT NOT NULL,
  address VARCHAR(66) NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (transaction_version, index),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);
CREATE INDEX wsc_addr_type_ver_index ON write_set_changes (address, transaction_version DESC);
CREATE INDEX wsc_insat_index ON write_set_changes (inserted_at);
-- move modules in write set changes
CREATE TABLE move_modules (
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  name TEXT NOT NULL,
  address VARCHAR(66) NOT NULL,
  bytecode bytea,
  friends jsonb,
  exposed_functions jsonb,
  structs jsonb,
  is_deleted BOOLEAN NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (transaction_version, write_set_change_index),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);
CREATE INDEX mm_addr_name_ver_index ON move_modules (address, name, transaction_version);
CREATE INDEX mm_insat_index ON move_modules (inserted_at);
-- move resources in write set changes
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
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (transaction_version, write_set_change_index),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);
CREATE INDEX mr_addr_mod_name_ver_index ON move_resources (address, module, name, transaction_version);
CREATE INDEX mr_insat_index ON move_resources (inserted_at);
-- table items in write set changes
CREATE TABLE table_items (
  key text NOT NULL,
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  table_handle VARCHAR(66) NOT NULL,
  decoded_key jsonb NOT NULL,
  decoded_value jsonb,
  is_deleted BOOLEAN NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (transaction_version, write_set_change_index),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);
CREATE INDEX ti_hand_ver_key_index ON table_items (table_handle, transaction_version);
CREATE INDEX ti_insat_index ON table_items (inserted_at);
-- table metadatas from table items
CREATE TABLE table_metadatas (
  handle VARCHAR(66)  PRIMARY KEY NOT NULL,
  key_type text NOT NULL,
  value_type text NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX tm_h_index ON table_metadatas (handle);
CREATE INDEX tm_insat_index ON table_metadatas (inserted_at);
CREATE TABLE ledger_infos (chain_id BIGINT PRIMARY KEY NOT NULL);
CREATE UNIQUE INDEX li_c_index ON ledger_infos (chain_id);

-- Your SQL goes here
-- Tracks latest processed version per processor
CREATE TABLE processor_status (
  processor VARCHAR(50)  PRIMARY KEY NOT NULL,
  last_success_version BIGINT NOT NULL,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ps_p_index ON processor_status (processor);