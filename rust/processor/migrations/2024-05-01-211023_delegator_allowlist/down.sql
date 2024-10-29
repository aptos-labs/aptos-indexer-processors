-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS delegated_staking_pool_allowlist;
DROP TABLE IF EXISTS current_delegated_staking_pool_allowlist;
ALTER TABLE delegated_staking_pools DROP COLUMN IF EXISTS allowlist_enabled;