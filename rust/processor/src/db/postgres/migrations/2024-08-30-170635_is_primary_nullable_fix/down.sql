-- This file should undo anything in `up.sql`
ALTER TABLE IF EXISTS current_fungible_asset_balances
ALTER COLUMN is_primary DROP NOT NULL;
ALTER TABLE IF EXISTS current_fungible_asset_balances
ALTER COLUMN amount DROP NOT NULL;