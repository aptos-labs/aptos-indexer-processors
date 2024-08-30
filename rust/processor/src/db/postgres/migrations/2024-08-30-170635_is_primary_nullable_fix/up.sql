-- Your SQL goes here
ALTER TABLE IF EXISTS current_fungible_asset_balances
ALTER COLUMN is_primary
SET NOT NULL;
ALTER TABLE IF EXISTS current_fungible_asset_balances
ALTER COLUMN amount
SET NOT NULL;