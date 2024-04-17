-- Your SQL goes here
ALTER TABLE fungible_asset_metadata
ADD COLUMN supply_v2 NUMERIC,
ADD COLUMN maximum_v2 NUMERIC;

ALTER TABLE current_token_datas_v2
ALTER COLUMN supply DROP NOT NULL,
ALTER COLUMN decimals DROP NOT NULL;

ALTER TABLE token_datas_v2
ALTER COLUMN supply DROP NOT NULL,
ALTER COLUMN decimals DROP NOT NULL;