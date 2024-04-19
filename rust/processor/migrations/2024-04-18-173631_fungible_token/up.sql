-- Your SQL goes here
ALTER TABLE fungible_asset_metadata
ADD COLUMN supply_v2 NUMERIC,
ADD COLUMN maximum_v2 NUMERIC;