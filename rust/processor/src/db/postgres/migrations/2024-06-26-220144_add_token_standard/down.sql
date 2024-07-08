-- This file should undo anything in `up.sql`
ALTER TABLE current_unified_fungible_asset_balances_to_be_renamed
    DROP COLUMN IF EXISTS token_standard;