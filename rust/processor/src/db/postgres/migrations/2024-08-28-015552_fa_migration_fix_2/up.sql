-- Your SQL goes here
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed
ADD COLUMN IF NOT EXISTS asset_type VARCHAR(1000) GENERATED ALWAYS AS (COALESCE(asset_type_v1, asset_type_v2)) STORED;
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed
ADD COLUMN IF NOT EXISTS token_standard VARCHAR(10) GENERATED ALWAYS AS (
    CASE
      WHEN asset_type_v1 IS NOT NULL THEN 'v1'
      ELSE 'v2'
    END
  ) STORED;
ALTER TABLE current_fungible_asset_balances
  RENAME TO current_fungible_asset_balances_legacy;
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed
  RENAME TO current_fungible_asset_balances;