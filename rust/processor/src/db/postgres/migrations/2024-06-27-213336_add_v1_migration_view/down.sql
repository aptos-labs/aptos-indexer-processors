-- This file should undo anything in `up.sql`
DROP VIEW IF EXISTS legacy_migration_v1.current_collection_datas;
DROP INDEX IF EXISTS lm1_curr_cd_th_index;