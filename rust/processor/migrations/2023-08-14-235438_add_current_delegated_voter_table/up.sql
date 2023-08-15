-- Your SQL goes here
-- current delegated voters
CREATE TABLE IF NOT EXISTS current_delegated_voter (
  delegation_pool_address VARCHAR(66) NOT NULL,
  delegator_address VARCHAR(66) NOT NULL,
  table_handle VARCHAR(66) NOT NULL,
  voter VARCHAR(66),
  pending_voter VARCHAR(66),
  last_transaction_version BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (delegation_pool_address, delegator_address)
);
CREATE INDEX IF NOT EXISTS cdv_dpa_idx ON current_delegated_voter (delegation_pool_address);
CREATE INDEX IF NOT EXISTS cdv_da_index ON current_delegated_voter (delegator_address);
CREATE INDEX IF NOT EXISTS cdv_th_index ON current_delegated_voter (table_handle);
CREATE INDEX IF NOT EXISTS cdv_v_index ON current_delegated_voter (voter);
CREATE INDEX IF NOT EXISTS cdv_pv_index ON current_delegated_voter (pending_voter);