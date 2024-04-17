-- This file should undo anything in `up.sql`
ALTER TABLE fungible_asset_metadata
DROP COLUMN IF EXISTS is_token_v2;
ALTER TABLE objects
ADD COLUMN IF NOT EXISTS is_token BOOLEAN,
ADD COLUMN IF NOT EXISTS is_fungible_asset BOOLEAN;
ALTER TABLE current_objects
ADD COLUMN IF NOT EXISTS is_token BOOLEAN,
ADD COLUMN IF NOT EXISTS is_fungible_asset BOOLEAN;