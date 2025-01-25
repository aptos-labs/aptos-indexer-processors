-- This file should undo anything in `up.sql`
CREATE INDEX IF NOT EXISTS faa_si_index ON fungible_asset_activities (storage_id);