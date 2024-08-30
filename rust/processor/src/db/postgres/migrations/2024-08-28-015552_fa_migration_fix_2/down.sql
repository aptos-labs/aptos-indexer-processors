-- This file should undo anything in `up.sql`
ALTER TABLE current_fungible_asset_balances
  RENAME TO current_unified_fungible_asset_balances_to_be_renamed;
ALTER TABLE current_fungible_asset_balances_legacy
  RENAME TO current_fungible_asset_balances;
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed DROP COLUMN IF EXISTS asset_type;
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed DROP COLUMN IF EXISTS token_standard;