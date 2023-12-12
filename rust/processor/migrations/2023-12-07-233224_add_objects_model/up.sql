-- Your SQL goes here
ALTER TABLE objects ADD COLUMN IF NOT EXISTS is_token BOOLEAN;
ALTER TABLE objects ADD COLUMN IF NOT EXISTS is_fungible_asset BOOLEAN;
ALTER TABLE current_objects ADD COLUMN IF NOT EXISTS is_token BOOLEAN;
ALTER TABLE current_objects ADD COLUMN IF NOT EXISTS is_fungible_asset BOOLEAN;

ALTER TABLE block_metadata_transactions DROP CONSTRAINT IF EXISTS fk_versions;
ALTER TABLE move_modules DROP CONSTRAINT IF EXISTS fk_transaction_versions;
ALTER TABLE move_resources DROP CONSTRAINT IF EXISTS fk_transaction_versions;
ALTER TABLE table_items DROP CONSTRAINT IF EXISTS fk_transaction_versions;
ALTER TABLE write_set_changes DROP CONSTRAINT IF EXISTS fk_transaction_versions;
