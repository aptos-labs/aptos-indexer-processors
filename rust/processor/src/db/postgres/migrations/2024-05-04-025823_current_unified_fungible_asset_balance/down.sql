-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS current_unified_fungible_asset_balances;
DROP INDEX IF EXISTS cufab_owner_at_index;
DROP INDEX IF EXISTS cufab_insat_index;
