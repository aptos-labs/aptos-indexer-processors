-- This file should undo anything in `up.sql`
ALTER TABLE fungible_asset_metadata
DROP COLUMN supply_v2,
DROP COLUMN maximum_v2;