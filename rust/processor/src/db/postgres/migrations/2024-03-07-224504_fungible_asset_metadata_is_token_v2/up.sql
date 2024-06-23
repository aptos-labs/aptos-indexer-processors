-- Your SQL goes here
ALTER TABLE fungible_asset_metadata
ADD COLUMN IF NOT EXISTS is_token_v2 BOOLEAN;
ALTER TABLE objects
DROP COLUMN IF EXISTS is_fungible_asset,
DROP COLUMN IF EXISTS is_token;
ALTER TABLE current_objects
DROP COLUMN IF EXISTS is_fungible_asset,
DROP COLUMN IF EXISTS is_token;