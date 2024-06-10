-- Your SQL goes here
-- adding new fields to staking pool balances for display and handling inactive pools
ALTER TABLE current_delegated_staking_pool_balances
ADD COLUMN IF NOT EXISTS operator_commission_percentage NUMERIC NOT NULL,
  ADD COLUMN IF NOT EXISTS inactive_table_handle VARCHAR(66) NOT NULL,
  ADD COLUMN IF NOT EXISTS active_table_handle VARCHAR(66) NOT NULL;
CREATE INDEX IF NOT EXISTS cdspb_inactive_index ON current_delegated_staking_pool_balances (inactive_table_handle);
-- adding new fields to staking pool balances for display and handling inactive pools
ALTER TABLE delegated_staking_pool_balances
ADD COLUMN IF NOT EXISTS operator_commission_percentage NUMERIC NOT NULL,
  ADD COLUMN IF NOT EXISTS inactive_table_handle VARCHAR(66) NOT NULL,
  ADD COLUMN IF NOT EXISTS active_table_handle VARCHAR(66) NOT NULL;
-- add new field to composite primary key because technically a user could have inactive pools
ALTER TABLE current_delegator_balances
ADD COLUMN IF NOT EXISTS parent_table_handle VARCHAR(66) NOT NULL;

-- need this for delegation staking
CREATE OR REPLACE VIEW num_active_delegator_per_pool AS
SELECT pool_address,
  COUNT(DISTINCT delegator_address) AS num_active_delegator
FROM current_delegator_balances
WHERE shares > 0
  AND pool_type = 'active_shares'
GROUP BY 1;

-- need this for delegation staking
CREATE OR REPLACE VIEW delegator_distinct_pool AS
SELECT delegator_address,
  pool_address
FROM current_delegator_balances
WHERE shares > 0
GROUP BY 1,
  2;
-- new query for wallet
CREATE OR REPLACE VIEW address_events_summary AS
SELECT account_address,
  min(transaction_block_height) AS min_block_height,
  count(DISTINCT transaction_version) AS num_distinct_versions
FROM events
GROUP BY 1