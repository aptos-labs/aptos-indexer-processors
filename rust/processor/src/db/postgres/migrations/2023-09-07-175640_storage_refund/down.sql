-- This file should undo anything in `up.sql`
ALTER TABLE coin_activities DROP COLUMN IF EXISTS storage_refund_amount;
ALTER TABLE fungible_asset_activities DROP COLUMN IF EXISTS storage_refund_amount;