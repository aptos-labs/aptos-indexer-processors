ALTER TABLE current_ans_lookup
DROP COLUMN IF EXISTS is_deleted;

DROP TABLE IF EXISTS current_ans_primary_name;
DROP TABLE IF EXISTS ans_primary_name;
DROP TABLE IF EXISTS ans_lookup;