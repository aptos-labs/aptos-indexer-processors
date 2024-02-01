-- This file should undo anything in `up.sql`
-- estimates how much delegator has staked in a pool (currently supports active only)
DROP INDEX CONCURRENTLY IF EXISTS db_da_index;

DROP INDEX CONCURRENTLY IF EXISTS db_insat_index;
