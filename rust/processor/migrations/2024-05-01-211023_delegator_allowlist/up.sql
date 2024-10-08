-- Your SQL goes here
ALTER TABLE delegated_staking_pools
ADD COLUMN IF NOT EXISTS allowlist_enabled BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS current_delegated_staking_pool_allowlist (
  staking_pool_address VARCHAR(66) NOT NULL,
  delegator_address VARCHAR(66) NOT NULL,
  -- Used for soft delete. On chain, it's a delete operation.
  is_allowed BOOLEAN NOT NULL DEFAULT FALSE,
  last_transaction_version BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (delegator_address, staking_pool_address)
);

CREATE TABLE IF NOT EXISTS delegated_staking_pool_allowlist (
  staking_pool_address VARCHAR(66) NOT NULL,
  delegator_address VARCHAR(66) NOT NULL,
  -- Used for soft delete. On chain, it's a delete operation.
  is_allowed BOOLEAN NOT NULL DEFAULT FALSE,
  transaction_version BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, delegator_address, staking_pool_address)
);