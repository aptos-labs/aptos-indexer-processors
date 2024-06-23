-- Your SQL goes here
-- Rename asset_type and coin_type to v1 and v2, and make a generated asset_type to be v2 if exists, else v1.
DROP INDEX IF EXISTS cufab_owner_at_index;
ALTER TABLE current_unified_fungible_asset_balances
ALTER COLUMN asset_type DROP NOT NULL;
ALTER TABLE current_unified_fungible_asset_balances
  RENAME COLUMN asset_type TO asset_type_v2;
ALTER TABLE current_unified_fungible_asset_balances
  RENAME COLUMN coin_type TO asset_type_v1;
ALTER TABLE current_unified_fungible_asset_balances
ADD COLUMN asset_type VARCHAR(1000) GENERATED ALWAYS AS (COALESCE(asset_type_v2, asset_type_v1)) STORED;
CREATE INDEX IF NOT EXISTS cufab_owner_at_index ON current_unified_fungible_asset_balances (owner_address, asset_type);
-- Rename table to set expectation that we'll rename this table to current_fungible_asset_balances after testing
ALTER TABLE current_unified_fungible_asset_balances
  RENAME TO current_unified_fungible_asset_balances_to_be_renamed;