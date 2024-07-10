-- Your SQL goes here
ALTER TABLE IF EXISTS fungible_asset_activities ALTER COLUMN asset_type DROP NOT NULL;
ALTER TABLE IF EXISTS fungible_asset_activities ALTER COLUMN owner_address DROP NOT NULL;