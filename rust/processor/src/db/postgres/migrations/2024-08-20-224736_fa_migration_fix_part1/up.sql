-- Your SQL goes here
--truncate table because there are a bunch of changes that might pose a problem with existing data
TRUNCATE TABLE current_unified_fungible_asset_balances_to_be_renamed;
-- removing generated fields because we're redoing them
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed DROP COLUMN IF EXISTS asset_type;
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed DROP COLUMN IF EXISTS token_standard;