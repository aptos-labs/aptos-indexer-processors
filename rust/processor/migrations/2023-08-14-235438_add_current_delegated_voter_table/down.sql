-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS current_delegated_voter;
DROP INDEX IF EXISTS cdv_dpa_idx;
DROP INDEX IF EXISTS cdv_da_index;
DROP INDEX IF EXISTS cdv_th_index;
DROP INDEX IF EXISTS cdv_v_index;
DROP INDEX IF EXISTS cdv_pv_index;