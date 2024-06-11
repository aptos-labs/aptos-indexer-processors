-- This file should undo anything in `up.sql`
DROP INDEX IF EXISTS tsi_insat_index;
DROP INDEX IF EXISTS esi_insat_index;
DROP INDEX IF EXISTS wsi_insat_index;
DROP TABLE IF EXISTS transaction_size_info;
DROP TABLE IF EXISTS event_size_info;
DROP TABLE IF EXISTS write_set_size_info;
