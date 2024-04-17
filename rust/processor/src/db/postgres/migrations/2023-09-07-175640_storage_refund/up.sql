-- Your SQL goes here
ALTER TABLE coin_activities
ADD COLUMN IF NOT EXISTS storage_refund_amount NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE fungible_asset_activities
ADD COLUMN IF NOT EXISTS storage_refund_amount NUMERIC NOT NULL DEFAULT 0;