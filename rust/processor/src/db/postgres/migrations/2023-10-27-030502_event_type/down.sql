-- This file should undo anything in `up.sql`
DROP INDEX IF EXISTS ev_itype_index;
ALTER TABLE events DROP COLUMN IF EXISTS indexed_type;
DROP TABLE IF EXISTS spam_assets;