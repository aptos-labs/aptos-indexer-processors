ALTER TABLE current_ans_lookup
DROP COLUMN is_primary,
DROP COLUMN is_deleted;

DROP TABLE IF EXISTS ans_lookup;