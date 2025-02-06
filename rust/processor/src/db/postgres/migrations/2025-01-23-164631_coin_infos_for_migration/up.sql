-- Create coin to fungible asset mapping table for migration
CREATE TABLE IF NOT EXISTS fungible_asset_to_coin_mappings (
  fungible_asset_metadata_address VARCHAR(66) NOT NULL PRIMARY KEY,
  coin_type VARCHAR(1000) NOT NULL,
  last_transaction_version BIGINT NOT NULL
);
-- Create indices
CREATE INDEX IF NOT EXISTS fatcm_coin_type_idx ON fungible_asset_to_coin_mappings (coin_type);