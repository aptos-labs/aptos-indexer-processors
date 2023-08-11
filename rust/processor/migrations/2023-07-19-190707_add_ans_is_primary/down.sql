ALTER TABLE current_ans_lookup
DROP COLUMN IF EXISTS is_primary,
DROP COLUMN IF EXISTS is_deleted;

DROP TABLE IF EXISTS ans_lookup;