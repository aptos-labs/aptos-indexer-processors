-- Your SQL goes here
ALTER TABLE current_token_datas_v2
ADD COLUMN IF NOT EXISTS is_deleted_v2 BOOLEAN;