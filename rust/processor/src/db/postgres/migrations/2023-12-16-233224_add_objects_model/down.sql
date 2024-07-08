-- This file should undo anything in `up.sql`
ALTER TABLE objects DROP COLUMN IF EXISTS is_token;
ALTER TABLE objects DROP COLUMN IF EXISTS is_fungible_asset;
ALTER TABLE current_objects DROP COLUMN IF EXISTS is_token;
ALTER TABLE current_objects DROP COLUMN IF EXISTS is_fungible_asset;