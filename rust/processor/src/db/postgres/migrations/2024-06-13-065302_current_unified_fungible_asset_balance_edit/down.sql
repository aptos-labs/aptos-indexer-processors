-- This file should undo anything in `up.sql`
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed
  RENAME TO current_unified_fungible_asset_balances;
DROP INDEX IF EXISTS cufab_owner_at_index;
ALTER TABLE current_unified_fungible_asset_balances DROP COLUMN asset_type;
ALTER TABLE current_unified_fungible_asset_balances
  RENAME COLUMN asset_type_v2 TO asset_type;
ALTER TABLE current_unified_fungible_asset_balances
  RENAME COLUMN asset_type_v1 TO coin_type;
ALTER TABLE current_unified_fungible_asset_balances
ALTER COLUMN asset_type
SET NOT NULL;
CREATE INDEX IF NOT EXISTS cufab_owner_at_index ON current_unified_fungible_asset_balances (owner_address, asset_type);