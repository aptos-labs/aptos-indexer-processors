-- Your SQL goes here
-- removing generated fields because we're redoing them
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed DROP COLUMN IF EXISTS asset_type;
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed DROP COLUMN IF EXISTS token_standard;