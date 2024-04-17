-- This file should undo anything in `up.sql`
-- estimates how much delegator has staked in a pool (currently supports active only)
DROP INDEX IF EXISTS db_da_index;
DROP INDEX IF EXISTS db_insat_index;
DROP TABLE IF EXISTS delegator_balances;