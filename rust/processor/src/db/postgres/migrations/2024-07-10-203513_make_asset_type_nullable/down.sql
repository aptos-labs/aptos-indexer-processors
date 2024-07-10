-- This file should undo anything in `up.sql`
ALTER TABLE IF EXISTS fungible_asset_activities ALTER COLUMN asset_type SET NOT NULL;