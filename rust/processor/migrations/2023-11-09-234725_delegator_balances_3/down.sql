-- This file should undo anything in `up.sql`
-- estimates how much delegator has staked in a pool (currently supports active only)

DROP INDEX CONCURRENTLY IF EXISTS cdb_insat_index;
