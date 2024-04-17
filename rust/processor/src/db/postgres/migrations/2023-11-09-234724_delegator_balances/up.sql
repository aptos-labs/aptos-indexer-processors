-- Your SQL goes here
-- oops made a mistake here before
DROP INDEX IF EXISTS cdb_insat_index;
CREATE INDEX cdb_insat_index ON current_delegator_balances (inserted_at);
-- estimates how much delegator has staked in a pool (currently supports active only)
CREATE TABLE IF NOT EXISTS delegator_balances (
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  delegator_address VARCHAR(66) NOT NULL,
  pool_address VARCHAR(66) NOT NULL,
  pool_type VARCHAR(100) NOT NULL,
  table_handle VARCHAR(66) NOT NULL,
  shares NUMERIC NOT NULL,
  parent_table_handle VARCHAR(66) NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX IF NOT EXISTS db_da_index ON delegator_balances (delegator_address);
CREATE INDEX IF NOT EXISTS db_insat_index ON delegator_balances (inserted_at);