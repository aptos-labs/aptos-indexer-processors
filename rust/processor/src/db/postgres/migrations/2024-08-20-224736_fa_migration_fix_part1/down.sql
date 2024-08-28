-- Your SQL goes here
-- removing generated fields because we're redoing them
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed
ADD COLUMN IF NOT EXISTS asset_type VARCHAR(1000) GENERATED ALWAYS AS (COALESCE(asset_type_v2, asset_type_v1)) STORED;
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed
ADD COLUMN IF NOT EXISTS token_standard VARCHAR(10) GENERATED ALWAYS AS (
    CASE
      WHEN asset_type_v2 IS NOT NULL THEN 'v2'
      ELSE 'v1'
    END
  ) STORED;